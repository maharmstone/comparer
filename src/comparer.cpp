#include <windows.h>
#include <tdscpp.h>
#include <iostream>
#include <string>
#include <list>
#include <thread>
#include <optional>
#include <mutex>
#include <functional>
#include <array>
#include <fmt/format.h>
#include <fmt/compile.h>

using namespace std;

static const string DB_APP = "Janus";

static unsigned int log_id = 0;
static string db_server, db_username, db_password;

enum class change {
	modified,
	added,
	removed
};

class last_error : public std::exception {
public:
	last_error(const std::string_view& function, int le) {
		std::string nice_msg;

		{
			char16_t* fm;

			if (FormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr,
				le, 0, reinterpret_cast<LPWSTR>(&fm), 0, nullptr)) {
				try {
					std::u16string_view s = fm;

					while (!s.empty() && (s[s.length() - 1] == u'\r' || s[s.length() - 1] == u'\n')) {
						s.remove_suffix(1);
					}

					nice_msg = tds::utf16_to_utf8(s);
				} catch (...) {
					LocalFree(fm);
					throw;
				}

				LocalFree(fm);
				}
		}

		msg = std::string(function) + " failed (error " + std::to_string(le) + (!nice_msg.empty() ? (", " + nice_msg) : "") + ").";
	}

	const char* what() const noexcept {
		return msg.c_str();
	}

private:
	std::string msg;
};

class _formatted_error : public std::exception {
public:
	template<typename T, typename... Args>
	_formatted_error(const T& s, Args&&... args) {
		msg = fmt::format(s, std::forward<Args>(args)...);
	}

	const char* what() const noexcept {
		return msg.c_str();
	}

private:
	std::string msg;
};

#define formatted_error(s, ...) _formatted_error(FMT_COMPILE(s), ##__VA_ARGS__)

class result {
public:
	result(int query, const string& primary_key, enum change change, unsigned int col,
		   const tds::value& value1, const tds::value& value2, const tds::value& col_name) :
		query(query), primary_key(primary_key), change(change), col(col), value1(value1), value2(value2), col_name(col_name) {
	}

	int query;
	string primary_key;
	enum change change;
	unsigned int col;
	tds::value value1, value2, col_name;
};

class win_event {
public:
	win_event() {
		h = CreateEvent(nullptr, false, false, nullptr);

		if (!h)
			throw last_error("CreateEvent", GetLastError());
	}

	~win_event() {
		CloseHandle(h);
	}

	void set() {
		SetEvent(h);
	}

	void wait() {
		auto ret = WaitForSingleObject(h, INFINITE);

		if (ret != WAIT_OBJECT_0)
			throw formatted_error("CreateEvent returned {}.", ret);
	}

private:
	HANDLE h;
};

class sql_thread {
public:
	sql_thread(const string_view& query) : finished(false), query(query), tds(db_server, db_username, db_password, DB_APP), t([](sql_thread* st) {
			st->run();
		}, this) {
	}

	~sql_thread() {
		t.join();
	}

	void run() {
		try {
			tds::query sq(tds, query);

			auto b = sq.fetch_row();

			auto num_col = sq.num_columns();

			if (b) {
				do {
					size_t num_res;

					do {
						lock_guard<mutex> guard(lock);

						num_res = results.size();
					} while (num_res > 100000 && !finished);

					if (finished)
						break;

					{
						lock_guard<mutex> guard(lock);

						results.emplace_back();

						auto& v = results.back();

						v.reserve(num_col);

						for (uint16_t i = 0; i < num_col; i++) {
							v.emplace_back(sq[i]);
						}
					}

					event.set();
				} while (!finished && sq.fetch_row());
			}
		} catch (...) {
			ex = current_exception();
		}

		finished = true;
		event.set();
	}

	void wait_for(const function<void()>& func) {
		event.wait();

		lock_guard<mutex> guard(lock);

		func();
	}

	bool finished;
	string query;
	tds::tds tds;
	thread t;
	exception_ptr ex;
	list<vector<tds::column>> results;
	mutex lock;
	win_event event;
};

static void create_queries(tds::tds& tds, const string_view& tbl1, const string_view& tbl2,
						   string& q1, string& q2, unsigned int& pk_columns) {
	vector<string> cols;
	int64_t object_id;

	pk_columns = 0;

	// FIXME - extract prefix if three- or four-part name

	{
		tds::query sq(tds, "SELECT OBJECT_ID(?)", tbl1);

		if (!sq.fetch_row() || sq[0].is_null)
			throw formatted_error("Could not get object ID for {}.", tbl1);

		object_id = (int64_t)sq[0];
	}

	{
		tds::query sq(tds, R"(SELECT columns.name
FROM sys.index_columns
JOIN sys.indexes ON indexes.object_id = index_columns.object_id AND indexes.index_id = index_columns.index_id
JOIN sys.columns ON columns.object_id = index_columns.object_id AND columns.column_id = index_columns.column_id
WHERE index_columns.object_id = ? AND indexes.is_primary_key = 1
ORDER BY index_columns.index_column_id)", object_id);

		while (sq.fetch_row()) {
			cols.emplace_back(tds::escape((string)sq[0]));
			pk_columns++;
		}
	}

	if (cols.empty())
		throw formatted_error("No primary key found for {}.", tbl1);

	{
		tds::query sq(tds, R"(SELECT columns.name
FROM sys.columns
JOIN sys.indexes ON indexes.object_id = columns.object_id AND indexes.is_primary_key = 1
LEFT JOIN sys.index_columns ON index_columns.object_id = columns.object_id AND index_columns.index_id = indexes.index_id AND index_columns.column_id = columns.column_id
WHERE columns.object_id = ? AND index_columns.column_id IS NULL
ORDER BY columns.column_id)", object_id);

		while (sq.fetch_row()) {
			cols.emplace_back(tds::escape((string)sq[0]));
		}
	}

	// FIXME - PKs with DESC element?

	if (cols.empty())
		throw formatted_error("No columns returned for {}.", tbl1);

	// FIXME - skip "Data Load Date" etc.

	for (const auto& col : cols) {
		if (q1.empty())
			q1 = "SELECT ";
		else
			q1 += ", ";

		q1 += col;

		if (q2.empty())
			q2 = "SELECT ";
		else
			q2 += ", ";

		q2 += col;
	}

	q1 += " FROM ";
	q1 += tbl1;
	q1 += " ORDER BY ";

	q2 += " FROM ";
	q2 += tbl2;
	q2 += " ORDER BY ";

	for (unsigned int i = 0; i < pk_columns; i++) {
		if (i != 0) {
			q1 += ", ";
			q2 += ", ";
		}

		q1 += cols[i];
		q2 += cols[i];
	}
}

static int compare_value(const tds::column& val1, const tds::column& val2) {
	switch (val1.type) {
		case tds::sql_type::INTN:
		case tds::sql_type::TINYINT:
		case tds::sql_type::SMALLINT:
		case tds::sql_type::INT:
		case tds::sql_type::BIGINT: {
			auto v1 = (int64_t)val1;
			auto v2 = (int64_t)val2;

			if (v1 == v2)
				return 0;

			return v1 < v2 ? -1 : 1;
		}

		case tds::sql_type::DATE: {
			auto v1 = (chrono::year_month_day)val1;
			auto v2 = (chrono::year_month_day)val2;

			if (v1 == v2)
				return 0;

			return v1 < v2 ? -1 : 1;
		}

		// FIXME - IMAGE
		// FIXME - TEXT
		// FIXME - UNIQUEIDENTIFIER
		// FIXME - TIME
		// FIXME - DATETIME2
		// FIXME - DATETIMEOFFSET
		// FIXME - BIT
		// FIXME - DATETIM4
		// FIXME - REAL
		// FIXME - MONEY
		// FIXME - DATETIME
		// FIXME - FLOAT
		// FIXME - SQL_VARIANT
		// FIXME - NTEXT
		// FIXME - BITN
		// FIXME - DECIMAL
		// FIXME - NUMERIC
		// FIXME - FLTN
		// FIXME - MONEYN
		// FIXME - DATETIMN
		// FIXME - SMALLMONEY
		// FIXME - VARBINARY
		// FIXME - VARCHAR
		// FIXME - BINARY
		// FIXME - CHAR
		// FIXME - NVARCHAR
		// FIXME - NCHAR
		// FIXME - UDT
		// FIXME - XML

		default:
			throw formatted_error("Comparison for type {} unimplemented.", val1.type);
	}
}

static int compare_pks(const vector<tds::column>& row1, const vector<tds::column>& row2, unsigned int pk_columns) {
	for (unsigned int i = 0; i < pk_columns; i++) {
		auto ret = compare_value(row1[i], row2[i]);

		if (ret != 0)
			return ret;
	}

	return 0;
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

static void do_compare(unsigned int num) {
	unsigned int num_rows1 = 0, num_rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0, rows_since_update = 0;
	list<result> res;

	tds::tds tds(db_server, db_username, db_password, DB_APP);

	string q1, q2;
	unsigned int pk_columns;

	{
		optional<tds::query> sq2(in_place, tds, "SELECT type, query1, query2, table1, table2 FROM Comparer.queries WHERE id=?", num);

		auto& sq = sq2.value();

		if (!sq.fetch_row())
			throw runtime_error("Unable to find entry in Comparer.queries");

		auto type = (string)sq[0];

		if (type == "query") {
			if (sq[1].is_null)
				throw runtime_error("query1 is NULL");

			if (sq[2].is_null)
				throw runtime_error("query2 is NULL");

			q1 = (string)sq[1];
			q2 = (string)sq[2];

			pk_columns = 1;
		} else if (type == "table") {
			if (sq[3].is_null)
				throw runtime_error("table1 is NULL");

			if (sq[4].is_null)
				throw runtime_error("table2 is NULL");

			auto tbl1 = (string)sq[3];
			auto tbl2 = (string)sq[4];

			sq2.reset();

			create_queries(tds, tbl1, tbl2, q1, q2, pk_columns);
		} else
			throw formatted_error("Unsupported type {}.", type);
	}

	bool t1_finished = false, t2_finished = false, t1_done = false, t2_done = false;
	vector<tds::column> row1, row2;
	list<vector<tds::column>> rows1, rows2;

	sql_thread t1(q1);
	sql_thread t2(q2);

	auto t1_fetch = [&]() {
		while (rows1.empty() && !t1_finished) {
			t1_finished = t1_done;

			if (t1_finished)
				return;

			t1.wait_for([&]() {
				t1_done = t1.finished;

				if (!t1.results.empty())
					rows1.splice(rows1.end(), t1.results);

				if (t1.finished && t1.ex)
					rethrow_exception(t1.ex);
			});

			if (rows1.empty() && t1_done) {
				t1_finished = true;
				return;
			}
		}

		row1 = move(rows1.front());
		rows1.pop_front();
	};

	auto t2_fetch = [&]() {
		while (rows2.empty() && !t2_finished) {
			t2_finished = t2_done;

			if (t2_finished)
				return;

			t2.wait_for([&]() {
				t2_done = t2.finished;

				if (!t2.results.empty())
					rows2.splice(rows2.end(), t2.results);

				if (t2.finished && t2.ex)
					rethrow_exception(t2.ex);
			});

			if (rows2.empty() && t2_done) {
				t2_finished = true;
				return;
			}
		}

		row2 = move(rows2.front());
		rows2.pop_front();
	};

	try {
		t1_fetch();
		t2_fetch();

		{
			tds::query sq(tds, "INSERT INTO Comparer.log(date, query, success, error) VALUES(GETDATE(), ?, 0, 'Interrupted.'); SELECT SCOPE_IDENTITY()", num);

			if (!sq.fetch_row())
				throw runtime_error("Error creating log entry.");

			log_id = (unsigned int)sq[0];
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

		while (!t1_finished || !t2_finished) {
			if (!t1_finished && !t2_finished) {
				auto cmp = compare_pks(row1, row2, pk_columns);

				// FIXME - what about tables made up solely of primary key?

				if (cmp == 0) {
					bool changed = false;
					string pk;

					for (unsigned int i = pk_columns; i < row1.size(); i++) {
						const auto& v1 = row1[i];
						const auto& v2 = row2[i];

						if ((!v1.is_null && v2.is_null) || (!v2.is_null && v1.is_null) || (!v1.is_null && !v2.is_null && compare_value(v1, v2))) {
							if (pk.empty())
								pk = make_pk_string(row1, pk_columns);

							res.emplace_back(num, pk, change::modified, i + 1, v1, v2, v1.name);
							changed = true;
						}
					}

					if (changed)
						changed_rows++;

					num_rows1++;
					num_rows2++;

					t1_fetch();
					t2_fetch();
				} else if (cmp < 0) {
					const auto& pk = make_pk_string(row1, pk_columns);

					for (unsigned int i = pk_columns; i < row1.size(); i++) {
						const auto& v1 = row1[i];

						if (v1.is_null)
							res.emplace_back(num, pk, change::removed, i + 1, nullptr, nullptr, v1.name);
						else
							res.emplace_back(num, pk, change::removed, i + 1, (string)v1, nullptr, v1.name);
					}

					removed_rows++;
					num_rows1++;

					t1_fetch();
				} else {
					const auto& pk = make_pk_string(row2, pk_columns);

					for (unsigned int i = pk_columns; i < row2.size(); i++) {
						const auto& v2 = row2[i];

						if (v2.is_null)
							res.emplace_back(num, pk, change::added, i + 1, nullptr, nullptr, v2.name);
						else
							res.emplace_back(num, pk, change::added, i + 1, nullptr, (string)v2, v2.name);
					}

					added_rows++;
					num_rows2++;

					t2_fetch();
				}
			} else if (!t1_finished) {
				const auto& pk = make_pk_string(row1, pk_columns);

				for (unsigned int i = 1; i < row1.size(); i++) {
					const auto& v1 = row1[i];

					if (v1.is_null)
						res.emplace_back(num, pk, change::removed, i + 1, nullptr, nullptr, v1.name);
					else
						res.emplace_back(num, pk, change::removed, i + 1, (string)v1, nullptr, v1.name);
				}

				removed_rows++;
				num_rows1++;

				t1_fetch();
			} else {
				const auto& pk = make_pk_string(row2, pk_columns);

				for (unsigned int i = 1; i < row2.size(); i++) {
					const auto& v2 = row2[i];

					if (v2.is_null)
						res.emplace_back(num, pk, change::added, i + 1, nullptr, nullptr, v2.name);
					else
						res.emplace_back(num, pk, change::added, i + 1, nullptr, (string)v2, v2.name);
				}

				added_rows++;
				num_rows2++;

				t2_fetch();
			}

			if (res.size() > 10000) { // flush
				vector<vector<tds::value>> v;

				v.reserve(res.size());

				while (!res.empty()) {
					auto r = move(res.front());
					res.pop_front();

					v.push_back({r.query, r.primary_key, r.change == change::added ? "added" : (r.change == change::removed ? "removed" : "modified"), r.col, r.value1, r.value2, r.col_name});
				}

				tds.bcp(u"Comparer.results", array{ u"query", u"primary_key", u"change", u"col", u"value1", u"value2", u"col_name" }, v);

				tds.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE() WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);

				rows_since_update = 0;
			} else if (rows_since_update > 1000) {
				tds.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE() WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);

				rows_since_update = 0;
			} else
				rows_since_update++;
		}

		if (!res.empty()) {
			vector<vector<tds::value>> v;

			v.reserve(res.size());

			while (!res.empty()) {
				auto r = move(res.front());
				res.pop_front();

				v.push_back({r.query, r.primary_key, r.change == change::added ? "added" : (r.change == change::removed ? "removed" : "modified"), r.col, r.value1, r.value2, r.col_name});
			}

			tds.bcp(u"Comparer.results", array{ u"query", u"primary_key", u"change", u"col", u"value1", u"value2", u"col_name" }, v);
		}

		tds.run("UPDATE Comparer.log SET success=1, rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE(), error=NULL WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);
	} catch (...) {
		t1.finished = true;
		t2.finished = true;
		throw;
	}
}

static optional<u16string> get_environment_variable(const u16string& name) {
	auto len = GetEnvironmentVariableW((WCHAR*)name.c_str(), nullptr, 0);

	if (len == 0) {
		if (GetLastError() == ERROR_ENVVAR_NOT_FOUND)
			return nullopt;

		return u"";
	}

	u16string ret(len, 0);

	if (GetEnvironmentVariableW((WCHAR*)name.c_str(), (WCHAR*)ret.data(), len) == 0)
		throw last_error("GetEnvironmentVariableW", GetLastError());

	while (!ret.empty() && ret.back() == 0) {
		ret.pop_back();
	}

	return ret;
}

int main(int argc, char* argv[]) {
	unsigned int num;

	if (argc < 2) {
		cerr << "Usage: comparer.exe <query number>" << endl;
		return 1;
	}

	try {
		num = stoul(argv[1]);

		auto db_server_env = get_environment_variable(u"DB_RMTSERVER");

		if (!db_server_env.has_value())
			throw runtime_error("Environment variable DB_RMTSERVER not set.");

		db_server = tds::utf16_to_utf8(db_server_env.value());

		if (db_server == "(local)") // SQL Agent does this
			db_server = "localhost";

		auto db_username_env = get_environment_variable(u"DB_USERNAME");

		if (db_username_env.has_value())
			db_username = tds::utf16_to_utf8(db_username_env.value());

		auto db_password_env = get_environment_variable(u"DB_PASSWORD");

		if (db_password_env.has_value())
			db_password = tds::utf16_to_utf8(db_password_env.value());

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
