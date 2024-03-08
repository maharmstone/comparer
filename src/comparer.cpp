#include "comparer.h"
#include <iostream>
#include <optional>
#include <mutex>
#include <array>
#include <charconv>
#include <numeric>

using namespace std;

static constexpr string_view DB_APP = "Janus";

// FIXME - calculate this dynamically?
static constexpr unsigned int MAX_PACKETS = 262144; // 1 GB

static unsigned int log_id = 0;
static string db_server, db_username, db_password;

sql_thread::sql_thread(u16string_view query, unique_ptr<tds::tds>& tds) : finished(false), query(query), uptds(move(tds)) {
	t = jthread([&](stop_token stop, sql_thread* st) noexcept {
		st->run(stop);
	}, this);
}

void sql_thread::run(stop_token stop) noexcept {
	try {
		auto& tds = *uptds.get();

		tds::query sq(tds, tds::no_check{query});

		auto num_col = sq.num_columns();

		cols.reserve(num_col);

		for (uint16_t i = 0; i < num_col; i++) {
			cols.emplace_back(sq[i]);
		}

		auto b = sq.fetch_row();

		if (b) {
			do {
				{
					unique_lock ul(lock);

					cv.wait(ul, [&]() { return results.size() <= 100000 || finished || stop.stop_requested(); });
				}

				if (finished || stop.stop_requested())
					break;

				decltype(results) l;

				do {
					l.emplace_back();
					auto& v = l.back();

					v.reserve(num_col);

					for (uint16_t i = 0; i < num_col; i++) {
						v.emplace_back();

						auto& vb = v.back();

						vb.first.swap(sq[i].val);
						vb.second = sq[i].is_null;
					}
				} while (sq.fetch_row_no_wait());

				{
					lock_guard<mutex> guard(lock);

					results.splice(results.end(), l);
				}

				cv.notify_all();
			} while (!finished && !stop.stop_requested() && sq.fetch_row());
		}
	} catch (...) {
		ex = current_exception();
	}

	finished = true;

	lock_guard<mutex> guard(lock);
	cv.notify_all();
}

sql_thread::~sql_thread() {
	t.request_stop();
	cv.notify_all();
}

void sql_thread::wait_for(const invocable auto& func) {
	unique_lock<mutex> ul(lock);

	cv.wait(ul, [&]() {
		return finished || !results.empty();
	});

	func();
}

static string sanitize_identifier(string_view sv) {
	if (sv.empty() || sv.front() != '[')
		return string{sv};

	if (sv.back() != ']')
		throw formatted_error("Malformed identifier {}.", sv);

	sv.remove_prefix(1);
	sv.remove_suffix(1);

	string s;

	s.reserve(sv.size());

	for (unsigned int i = 0; i < sv.size(); i++) {
		if (sv[i] == '[' && i < sv.size() - 1 && sv[i+1] == '[') {
			s += '[';
			i++;
		} else
			s += sv[i];
	}

	return s;
}

template<typename T>
requires requires { std::to_string(T{}); }
static u16string to_u16string(const T& t) {
	return tds::utf8_to_utf16(std::to_string(t));
}

static void create_results_table(tds::tds& tds, const vector<pk_col>& pk,
								 unsigned int num, u16string& results_table,
								 bool pk_only) {
	u16string q;

	// FIXME - unique used as primary key
	// FIXME - name collisions

	results_table = u"Comparer.results" + to_u16string(num);

	q = u"CREATE TABLE " + results_table + u" (\n";

	for (const auto& p : pk) {
		q += tds::escape(p.name) + u" ";
		q += p.type;
		q += u" NOT NULL,\n";
	}

	q += u"change VARCHAR(10) NOT NULL,\n";

	if (!pk_only) {
		q += u"col SMALLINT NOT NULL,\n";
		q += u"col_name VARCHAR(128) NOT NULL,\n";
		q += u"value1 VARCHAR(MAX) NULL,\n";
		q += u"value2 VARCHAR(MAX) NULL,\n";
	}

	q += u"PRIMARY KEY (";

	bool first = true;

	for (const auto& p : pk) {
		if (!first)
			q += u", ";

		q += tds::escape(p.name);

		if (p.desc)
			q += u" DESC";

		first = false;
	}

	if (pk_only)
		q += u")\n";
	else
		q += u", col)\n";

	q += u");";

	{
		tds::trans trans(tds);

		tds.run(tds::no_check{u"DROP TABLE IF EXISTS " + results_table});
		tds.run(tds::no_check{q});

		tds.run("EXEC sys.sp_addextendedproperty @name = N'microsoft_database_tools_support', @value = NULL, @level0type = 'SCHEMA', @level0name = 'Comparer', @level1type = 'TABLE', @level1name = ?", u"results" + to_u16string(num));

		trans.commit();
	}
}

static u16string type_to_string(u16string_view name, int max_length, int precision, int scale) {
	u16string ret{tds::escape(name)};

	if (name == u"CHAR" || name == u"VARCHAR" || name == u"BINARY" || name == u"VARBINARY")
		ret += u"(" + (max_length == -1 ? u"MAX" : to_u16string(max_length)) + u")";
	else if (name == u"NCHAR" || name == u"NVARCHAR")
		ret += u"(" + (max_length == -1 ? u"MAX" : to_u16string(max_length / 2)) + u")";
	else if (name == u"DECIMAL" || name == u"NUMERIC")
		ret += u"(" + to_u16string(precision) + u"," + to_u16string(scale) + u")";
	else if ((name == u"TIME" || name == u"DATETIME2" || name == u"DATETIMEOFFSET") && scale != 7)
		ret += u"(" + to_u16string(scale) + u")";

	// FIXME - collation

	return ret;
}

static void create_queries(tds::tds& tds, u16string_view tbl1, u16string_view tbl2,
						   u16string& q1, u16string& q2, string& server1, string& server2,
						   unsigned int& pk_columns, vector<pk_col>& pk,
						   bool& pk_only) {
	vector<u16string> cols;
	int64_t object_id;

	pk_only = false;
	pk_columns = 0;

	auto onp = tds::parse_object_name(tbl1);

	u16string prefix;

	if (!onp.server.empty() || !onp.db.empty())
		prefix = u16string(onp.db) + u".";

	if (!onp.server.empty())
		server1 = sanitize_identifier(tds::utf16_to_utf8(onp.server));
	else
		server1 = db_server;

	{
		optional<tds::tds> tds2;

		if (!onp.server.empty())
			tds2.emplace(server1, db_username, db_password, DB_APP);

		tds::tds& t = !onp.server.empty() ? *tds2 : tds;

		{
			optional<tds::query> sq2;

			if (!onp.server.empty()) {
				sq2.emplace(t, tds::no_check{uR"(SELECT object_id
FROM )" + prefix + uR"(sys.objects
JOIN )" + prefix + uR"(sys.schemas ON schemas.schema_id = objects.schema_id
WHERE objects.name = PARSENAME(?, 1) AND
schemas.name = PARSENAME(?, 2))"}, tbl1, tbl1);
			} else
				sq2.emplace(t, tds::no_check{"SELECT OBJECT_ID(?)"}, tbl1);

			auto& sq = sq2.value();

			if (!sq.fetch_row() || sq[0].is_null)
				throw formatted_error("Could not get object ID for {}.", tds::utf16_to_utf8(tbl1));

			object_id = (int64_t)sq[0];
		}

		optional<int32_t> index_id;

		{
			tds::query sq(t, tds::no_check{uR"(SELECT columns.name,
	indexes.index_id,
	CASE WHEN types.is_user_defined = 0 THEN UPPER(types.name) ELSE types.name END,
	columns.max_length,
	columns.precision,
	columns.scale,
	index_columns.is_descending_key
FROM )" + prefix + uR"(sys.index_columns
JOIN )" + prefix + uR"(sys.indexes ON indexes.object_id = index_columns.object_id AND indexes.index_id = index_columns.index_id
JOIN )" + prefix + uR"(sys.columns ON columns.object_id = index_columns.object_id AND columns.column_id = index_columns.column_id
JOIN )" + prefix + uR"(sys.types ON types.user_type_id = columns.user_type_id
WHERE index_columns.object_id = ? AND indexes.is_primary_key = 1
ORDER BY index_columns.index_column_id)"}, object_id);

			while (sq.fetch_row()) {
				if (!index_id.has_value())
					index_id = (int32_t)sq[1];

				cols.emplace_back(tds::escape((u16string)sq[0]));

				auto type = type_to_string((u16string)sq[2], (int)sq[3], (int)sq[4], (int)sq[5]);

				pk.emplace_back((u16string)sq[0], type, (unsigned int)sq[6] != 0);

				pk_columns++;
			}
		}

		if (!index_id.has_value()) { // if no primary key, look for unique key
			tds::query sq(t, tds::no_check{uR"(SELECT columns.name, indexes.index_id
FROM )" + prefix + uR"(sys.indexes
JOIN )" + prefix + uR"(sys.index_columns ON index_columns.object_id = indexes.object_id AND index_columns.index_id = indexes.index_id
JOIN )" + prefix + uR"(sys.columns ON columns.object_id = indexes.object_id AND columns.column_id = index_columns.column_id
WHERE indexes.object_id = ? AND indexes.index_id = (
	SELECT MIN(index_id)
	FROM )" + prefix + uR"(sys.indexes
	WHERE object_id = ? AND
	is_unique = 1
)
ORDER BY index_columns.index_column_id)"}, object_id, object_id);

			while (sq.fetch_row()) {
				if (!index_id.has_value())
					index_id = (int32_t)sq[1];

				cols.emplace_back(tds::escape((u16string)sq[0]));
				pk_columns++;
			}
		}

		{
			tds::query sq(t, tds::no_check{uR"(SELECT columns.name
FROM )" + prefix + uR"(sys.columns
LEFT JOIN )" + prefix + uR"(sys.index_columns ON index_columns.object_id = columns.object_id AND index_columns.index_id = ? AND index_columns.column_id = columns.column_id
WHERE columns.object_id = ? AND index_columns.column_id IS NULL
ORDER BY columns.column_id)"}, index_id, object_id);

			while (sq.fetch_row()) {
				auto s = (u16string)sq[0];

				if (s == u"Data Load Date" || s == u"data_load_date" || s == u"Snapshot Created") // FIXME - option for this?
					continue;

				cols.emplace_back(tds::escape(s));
			}
		}
	}

	pk_only = pk_columns == cols.size();

	if (cols.empty())
		throw formatted_error("No columns returned for {}.", tds::utf16_to_utf8(tbl1));

	for (const auto& col : cols) {
		if (q1.empty())
			q1 = u"SELECT ";
		else
			q1 += u", ";

		q1 += col;

		if (q2.empty())
			q2 = u"SELECT ";
		else
			q2 += u", ";

		q2 += col;
	}

	q1 += u" FROM ";
	q1 += tbl1;
	q1 += u" ORDER BY ";

	q2 += u" FROM ";

	onp = tds::parse_object_name(tbl2);

	if (!onp.server.empty()) {
		server2 = sanitize_identifier(tds::utf16_to_utf8(onp.server));

		if (!onp.db.empty()) {
			q2 += onp.db;
			q2 += u".";
		}

		if (!onp.schema.empty()) {
			q2 += onp.schema;
			q2 += u".";
		}

		q2 += onp.name;
	} else {
		server2 = db_server;
		q2 += tbl2;
	}

	q2 += u" ORDER BY ";

	for (unsigned int i = 0; i < ((pk_columns == 0) ? cols.size() : pk_columns); i++) {
		if (i != 0) {
			q1 += u", ";
			q2 += u", ";
		}

		q1 += cols[i];
		q2 += cols[i];
	}
}

static weak_ordering compare_cols(const vector<tds::column>& row1, const vector<tds::column>& row2, unsigned int columns) {
	for (unsigned int i = 0; i < columns; i++) {
		if (row1[i].is_null || row2[i].is_null) {
			if (row1[i].is_null && row2[i].is_null)
				continue;
			else if (row1[i].is_null)
				return weak_ordering::less;
			else
				return weak_ordering::greater;
		}

		auto ret = row1[i] <=> row2[i];

		if (ret == partial_ordering::unordered)
			throw runtime_error("Unexpected partial_ordering::unordered while comparing primary keys.");

		if (ret == partial_ordering::less)
			return weak_ordering::less;
		else if (ret == partial_ordering::greater)
			return weak_ordering::greater;
	}

	return weak_ordering::equivalent;
}

static string make_pk_string(const vector<tds::column>& row, unsigned int pk_columns) {
	string ret;

	for (unsigned int i = 0; i < pk_columns; i++) {
		if (i != 0)
			ret += ",";

		ret += (string)row[i];
	}

	return ret;
}

static string pseudo_pk(unsigned int& rownum) {
	auto s = format("{}", rownum);

	rownum++;

	return s;
}

static bool value_cmp(const tds::value& v1, const tds::value& v2) {
	if (v1.type != tds::sql_type::FLOAT && v1.type != tds::sql_type::REAL && v1.type != tds::sql_type::FLTN)
		return v1 == v2;

	// for FLOATs, allow some leeway on values

	// FIXME - for REALs, use float rather than double

	auto d1 = (double)v1;
	auto d2 = (double)v2;

	if (d1 > -1.0e-10 && d1 < 1.0e-10)
		d1 = 0.0;

	if (d2 > -1.0e-10 && d2 < 1.0e-10)
		d2 = 0.0;

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

	auto i1 = *reinterpret_cast<int64_t*>(&d1);
	auto i2 = *reinterpret_cast<int64_t*>(&d2);

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

	auto diff = i1 - i2;

	// this should be about 10 s.f.

	return diff < 262144 && diff >= -262144;
}

void bcp_thread::run(stop_token stop) noexcept {
	static const unsigned int MAX_BUF_ROWS = 100000;

	try {
		tds::tds tds(db_server, db_username, db_password, DB_APP);

		do {
			decltype(res) local_res;
			bool wait;
			vector<u16string> columns;

			if (!pk.empty()) {
				columns.reserve(pk.size());

				for (const auto& p : pk) {
					columns.emplace_back(p);
				}

				columns.emplace_back(u"change");

				if (table_name.empty() || !pk_only) {
					columns.emplace_back(u"col");
					columns.emplace_back(u"value1");
					columns.emplace_back(u"value2");
					columns.emplace_back(u"col_name");
				}
			}

			{
				unique_lock ul(lock);

				cv.wait(ul, stop, [&]{ return !res.empty(); });

				local_res.splice(local_res.end(), res);

				// if buffer full, pause other threads by keeping hold of lock
				wait = res.size() + local_res.size() >= MAX_BUF_ROWS;
			}

			if (local_res.empty() && stop.stop_requested())
				break;

			optional<lock_guard<mutex>> lg;

			if (wait)
				lg.emplace(lock);

			while (!local_res.empty()) {
				decltype(local_res) res2;

				if (local_res.size() <= 10000)
					res2.swap(local_res);
				else {
					for (unsigned int i = 0; i < 10000; i++) {
						res2.push_back(move(local_res.front()));
						local_res.pop_front();
					}
				}

				if (table_name.empty())
					tds.bcp(u"Comparer.results", array{ u"query", u"primary_key", u"change", u"col", u"value1", u"value2", u"col_name" }, res2);
				else
					tds.bcp(table_name, columns, res2);
			}
		} while (true);
	} catch (...) {
		exc = current_exception();
	}
}

static void repartition_results_table(tds::tds& tds, unsigned int num) {
	unsigned int func_num, next_num;
	bool part_found;

	{
		tds::query sq(tds, "SELECT function_id FROM sys.partition_functions WHERE name = 'comparer_part_func'");

		if (!sq.fetch_row()) // not partitioned
			return;

		func_num = (unsigned int)sq[0];
	}

	{
		tds::query sq(tds, "SELECT * FROM sys.partition_range_values WHERE function_id = ? AND value = ?", func_num, num);

		part_found = sq.fetch_row();
	}

	if (!part_found) {
		tds.run("ALTER PARTITION SCHEME comparer_part_scheme NEXT USED [PRIMARY]");
		tds.run("ALTER PARTITION FUNCTION comparer_part_func() SPLIT RANGE(?)", num);
	}

	{
		// parameterization slows this down
		tds::query sq(tds, tds::no_check{"SELECT MIN(query) FROM Comparer.results WHERE query > " + to_string(num)});

		if (!sq.fetch_row())
			return;

		if (sq[0].is_null)
			return;

		next_num = (unsigned int)sq[0];
	}

	{
		tds::query sq(tds, "SELECT * FROM sys.partition_range_values WHERE function_id = ? AND value = ?", func_num, next_num);

		part_found = sq.fetch_row();
	}

	if (part_found)
		return;

	tds.run("ALTER PARTITION SCHEME comparer_part_scheme NEXT USED [PRIMARY]");
	tds.run("ALTER PARTITION FUNCTION comparer_part_func() SPLIT RANGE(?)", next_num);
}

static void delete_old_results(tds::tds& tds, unsigned int num) {
	bool partitioned;
	int32_t part_num;

	{
		tds::query sq(tds, "SELECT function_id FROM sys.partition_functions WHERE name = 'comparer_part_func'");

		partitioned = sq.fetch_row();
	}

	if (partitioned) {
		{
			tds::query sq(tds, "SELECT $PARTITION.comparer_part_func(?)", num);

			if (!sq.fetch_row())
				throw runtime_error("Unable to get partition number to truncate.");

			part_num = (int32_t)sq[0];
		}

		tds.run("TRUNCATE TABLE Comparer.results WITH (PARTITIONS(?))", part_num);

		return;
	}

	tds.run(R"(
WHILE 1 = 1
BEGIN
	BEGIN TRANSACTION;

	DELETE TOP (100000)
	FROM Comparer.results
	WHERE query=?;

	COMMIT;

	IF (SELECT COUNT(*) FROM Comparer.results WHERE query=?) = 0
		BREAK;
END
)", num, num);
}

static size_t row_byte_count(size_t total, const tds::value& v) {
	if (!v.is_null)
		total += v.val.size();

	return total;
}

static void do_compare(unsigned int num) {
	tds::tds tds(db_server, db_username, db_password, DB_APP);

	u16string q1, q2;
	string server1, server2;
	unsigned int pk_columns;
	vector<pk_col> pk;
	u16string results_table, tbl1, tbl2;
	bool pk_only = false;

	{
		tds::query sq(tds, u"SELECT table1, table2 FROM Comparer.queries WHERE id = ?", num);

		if (!sq.fetch_row())
			throw runtime_error("Unable to find entry in Comparer.queries");

		if (sq[0].is_null)
			throw runtime_error("table1 is NULL");

		if (sq[1].is_null)
			throw runtime_error("table2 is NULL");

		tbl1 = (u16string)sq[0];
		tbl2 = (u16string)sq[1];
	}

	create_queries(tds, tbl1, tbl2, q1, q2, server1, server2, pk_columns,
				   pk, pk_only);

	repartition_results_table(tds, num);

	list<vector<pair<tds::value_data_t, bool>>> rows1, rows2;

	if (!pk.empty())
		create_results_table(tds, pk, num, results_table, pk_only);

	auto opts1 = tds::options(server1, db_username, db_password, DB_APP);
	auto opts2 = tds::options(server2, db_username, db_password, DB_APP);

	opts1.rate_limit = MAX_PACKETS;
	opts2.rate_limit = MAX_PACKETS;

	auto tds1 = make_unique<tds::tds>(opts1);
	auto tds2 = make_unique<tds::tds>(opts2);

	sql_thread t1(q1, tds1);
	sql_thread t2(q2, tds2);
	bcp_thread b(results_table, pk, pk_only);

	auto fetch = [](auto& rows, bool& finished, bool& done, sql_thread& t, auto& cols) {
		while (rows.empty() && !finished) {
			finished = done;

			if (finished)
				return;

			t.wait_for([&]() noexcept {
				done = t.finished;

				if (!t.results.empty()) {
					rows.splice(rows.end(), t.results);
					t.cv.notify_all();
				}
			});

			if (t.finished && t.ex)
				rethrow_exception(t.ex);

			if (rows.empty() && done) {
				finished = true;
				return;
			}
		}

		auto& rf = rows.front();

		for (size_t i = 0; i < rf.size(); i++) {
			cols[i].val.swap(rf[i].first);
			cols[i].is_null = rf[i].second;
		}

		rows.pop_front();
	};

	unsigned int num_rows1 = 0, num_rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0;
	size_t bytes1 = 0, bytes2 = 0;

	try {
		unsigned int rows_since_update = 0, rownum = 0;
		bool t1_finished = false, t2_finished = false, t1_done = false, t2_done = false;

		{
			tds::query sq(tds, "INSERT INTO Comparer.log(date, query, success, error) OUTPUT inserted.id VALUES(GETDATE(), ?, 0, 'Interrupted.')", num);

			if (!sq.fetch_row())
				throw runtime_error("Error creating log entry.");

			log_id = (unsigned int)sq[0];
		}

		auto run = [&]<bool do_new> {
			if constexpr (!do_new)
				delete_old_results(tds, num);

			fetch(rows1, t1_finished, t1_done, t1, t1.cols);
			fetch(rows2, t2_finished, t2_done, t2, t2.cols);

			while (!t1_finished || !t2_finished) {
				list<vector<tds::value>> local_res;

				if (b.exc)
					rethrow_exception(b.exc);

				if (!t1_finished && !t2_finished) {
					bytes1 = accumulate(t1.cols.begin(), t1.cols.end(), bytes1, row_byte_count);
					bytes2 = accumulate(t2.cols.begin(), t2.cols.end(), bytes2, row_byte_count);

					auto cmp = compare_cols(t1.cols, t2.cols, pk_columns == 0 ? (unsigned int)t1.cols.size() : pk_columns);

					if (cmp == weak_ordering::equivalent) {
						if (pk_columns > 0) {
							bool changed = false;
							string pk;

							for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
								const auto& v1 = t1.cols[i];
								const auto& v2 = t2.cols[i];

								if ((!v1.is_null && v2.is_null) || (!v2.is_null && v1.is_null) || (!v1.is_null && !v2.is_null && !value_cmp(v1, v2))) {

									if constexpr (do_new) {
										vector<tds::value> v;

										v.reserve(pk_columns + 5);

										for (unsigned int j = 0; j < pk_columns; j++) {
											v.emplace_back(t1.cols[j]);
										}

										v.emplace_back("modified");
										v.emplace_back(i + 1);
										v.emplace_back(v1);
										v.emplace_back(v2);
										v.emplace_back(t1.cols[i].name);

										local_res.push_back(v);
									} else {
										if (pk.empty())
											pk = make_pk_string(t1.cols, pk_columns);

										local_res.push_back({num, pk, "modified", i + 1, v1, v2, t1.cols[i].name});
									}

									changed = true;
								}
							}

							if (changed)
								changed_rows++;
						}

						num_rows1++;
						num_rows2++;

						fetch(rows1, t1_finished, t1_done, t1, t1.cols);
						fetch(rows2, t2_finished, t2_done, t2, t2.cols);
					} else if (cmp == weak_ordering::less) {
						if constexpr (do_new) {
							vector<tds::value> v;

							v.reserve(pk_columns + 5);

							for (unsigned int j = 0; j < pk_columns; j++) {
								v.emplace_back(t1.cols[j]);
							}

							v.emplace_back("removed");

							if (pk_only)
								local_res.push_back(v);
							else {
								for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
									const auto& v1 = t1.cols[i];

									v.emplace_back(i + 1);

									if (v1.is_null)
										v.emplace_back(nullptr);
									else
										v.emplace_back(v1);

									v.emplace_back(nullptr);
									v.emplace_back(t1.cols[i].name);

									local_res.push_back(v);

									v.resize(pk_columns + 1);
								}
							}
						} else {
							const auto& pk = pk_columns == 0 ? pseudo_pk(rownum) : make_pk_string(t1.cols, pk_columns);

							if (pk_only)
								local_res.push_back({num, pk, "removed", 0, nullptr, nullptr, nullptr});
							else {
								for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
									const auto& v1 = t1.cols[i];

									if (v1.is_null)
										local_res.push_back({num, pk, "removed", i + 1, nullptr, nullptr, t1.cols[i].name});
									else
										local_res.push_back({num, pk, "removed", i + 1, v1, nullptr, t1.cols[i].name});
								}
							}
						}

						removed_rows++;
						num_rows1++;

						fetch(rows1, t1_finished, t1_done, t1, t1.cols);
					} else {
						if constexpr (do_new) {
							vector<tds::value> v;

							v.reserve(pk_columns + 5);

							for (unsigned int j = 0; j < pk_columns; j++) {
								v.emplace_back(t2.cols[j]);
							}

							v.emplace_back("added");

							if (pk_only)
								local_res.push_back(v);
							else {
								for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
									const auto& v2 = t2.cols[i];

									v.emplace_back(i + 1);
									v.emplace_back(nullptr);

									if (v2.is_null)
										v.emplace_back(nullptr);
									else
										v.emplace_back(v2);

									v.emplace_back(t2.cols[i].name);

									local_res.push_back(v);

									v.resize(pk_columns + 1);
								}
							}
						} else {
							const auto& pk = pk_columns == 0 ? pseudo_pk(rownum) : make_pk_string(t2.cols, pk_columns);

							if (pk_only)
								local_res.push_back({num, pk, "added", 0, nullptr, nullptr, nullptr});
							else {
								for (unsigned int i = pk_columns; i < t2.cols.size(); i++) {
									const auto& v2 = t2.cols[i];

									if (v2.is_null)
										local_res.push_back({num, pk, "added", i + 1, nullptr, nullptr, t2.cols[i].name});
									else
										local_res.push_back({num, pk, "added", i + 1, nullptr, v2, t2.cols[i].name});
								}
							}
						}

						added_rows++;
						num_rows2++;

						fetch(rows2, t2_finished, t2_done, t2, t2.cols);
					}
				} else if (!t1_finished) {
					bytes1 = accumulate(t1.cols.begin(), t1.cols.end(), bytes1, row_byte_count);

					if constexpr (do_new) {
						vector<tds::value> v;

						v.reserve(pk_columns + 5);

						for (unsigned int j = 0; j < pk_columns; j++) {
							v.emplace_back(t1.cols[j]);
						}

						v.emplace_back("removed");

						if (pk_only)
							local_res.push_back(v);
						else {
							for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
								const auto& v1 = t1.cols[i];

								v.emplace_back(i + 1);

								if (v1.is_null)
									v.emplace_back(nullptr);
								else
									v.emplace_back(v1);

								v.emplace_back(nullptr);
								v.emplace_back(t1.cols[i].name);

								local_res.push_back(v);

								v.resize(pk_columns + 1);
							}
						}
					} else {
						const auto& pk = pk_columns == 0 ? pseudo_pk(rownum) : make_pk_string(t1.cols, pk_columns);

						if (pk_only)
							local_res.push_back({num, pk, "removed", 0, nullptr, nullptr, nullptr});
						else {
							for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
								const auto& v1 = t1.cols[i];

								if (v1.is_null)
									local_res.push_back({num, pk, "removed", i + 1, nullptr, nullptr, t1.cols[i].name});
								else
									local_res.push_back({num, pk, "removed", i + 1, v1, nullptr, t1.cols[i].name});
							}
						}
					}

					removed_rows++;
					num_rows1++;

					fetch(rows1, t1_finished, t1_done, t1, t1.cols);
				} else {
					bytes2 = accumulate(t2.cols.begin(), t2.cols.end(), bytes2, row_byte_count);

					if constexpr (do_new) {
						vector<tds::value> v;

						v.reserve(pk_columns + 5);

						for (unsigned int j = 0; j < pk_columns; j++) {
							v.emplace_back(t2.cols[j]);
						}

						v.emplace_back("added");

						if (pk_only)
							local_res.push_back(v);
						else {
							for (unsigned int i = pk_columns; i < t1.cols.size(); i++) {
								const auto& v2 = t2.cols[i];

								v.emplace_back(i + 1);
								v.emplace_back(nullptr);

								if (v2.is_null)
									v.emplace_back(nullptr);
								else
									v.emplace_back(v2);

								v.emplace_back(t2.cols[i].name);

								local_res.push_back(v);

								v.resize(pk_columns + 1);
							}
						}
					} else {
						const auto& pk = pk_columns == 0 ? pseudo_pk(rownum) : make_pk_string(t2.cols, pk_columns);

						if (pk_only)
							local_res.push_back({num, pk, "added", 0, nullptr, nullptr, nullptr});
						else {
							for (unsigned int i = pk_columns; i < t2.cols.size(); i++) {
								const auto& v2 = t2.cols[i];

								if (v2.is_null)
									local_res.push_back({num, pk, "added", i + 1, nullptr, nullptr, t2.cols[i].name});
								else
									local_res.push_back({num, pk, "added", i + 1, nullptr, v2, t2.cols[i].name});
							}
						}
					}

					added_rows++;
					num_rows2++;

					fetch(rows2, t2_finished, t2_done, t2, t2.cols);
				}

				if (!local_res.empty()) {
					{
						lock_guard<mutex> lg(b.lock);

						b.res.splice(b.res.end(), local_res);
					}

					b.cv.notify_one();
				}

				if (rows_since_update > 1000) {
					tds.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, bytes1=?, bytes2=?, end_date=SYSDATETIME() WHERE id=?",
							num_rows1, num_rows2, changed_rows, added_rows, removed_rows, (int64_t)bytes1, (int64_t)bytes2, log_id);

					rows_since_update = 0;
				} else
					rows_since_update++;
			}
		};

		if (results_table.empty())
			run.operator()<false>();
		else
			run.operator()<true>();
	} catch (...) {
		t1.finished = true;
		t2.finished = true;
		throw;
	}

	b.t.request_stop();
	b.t.join();

	if (b.exc)
		rethrow_exception(b.exc);

	tds.run("UPDATE Comparer.log SET success=1, rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, bytes1=?, bytes2=?, end_date=SYSDATETIME(), error=NULL WHERE id=?",
			num_rows1, num_rows2, changed_rows, added_rows, removed_rows, (int64_t)bytes1, (int64_t)bytes2, log_id);
}

int main(int argc, char* argv[]) {
	unsigned int num;

	if (argc < 2) {
		cerr << "Usage: comparer.exe <query number>" << endl;
		return 1;
	}

	auto sv = string_view(argv[1]);

	auto [ptr, ec] = from_chars(sv.data(), sv.data() + sv.length(), num);

	if (ec != errc()) {
		cerr << format("Could not convert \"{}\" to integer.\n", sv);
		return 1;
	}

	try {
		auto db_server_env = getenv("DB_RMTSERVER");

		if (!db_server_env)
			throw runtime_error("Environment variable DB_RMTSERVER not set.");

		db_server = db_server_env;

		if (db_server == "(local)") // SQL Agent does this
			db_server = "localhost";

		auto db_username_env = getenv("DB_USERNAME");

		if (db_username_env)
			db_username = db_username_env;

		auto db_password_env = getenv("DB_PASSWORD");

		if (db_password_env)
			db_password = db_password_env;

		do_compare(num);
	} catch (const exception& e) {
		cerr << "Exception: " << e.what() << endl;

		try {
			tds::tds tds(db_server, db_username, db_password, DB_APP);

			if (log_id == 0)
				tds.run("INSERT INTO Comparer.log(query, success, error) VALUES(?, 0, ?)", num, e.what());
			else
				tds.run("UPDATE Comparer.log SET error=?, end_date=GETDATE() WHERE id=?", e.what(), log_id);
		} catch (...) {
		}

		return 1;
	}

	return 0;
}
