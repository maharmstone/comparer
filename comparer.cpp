#include <windows.h>
#include <tdscpp.h>
#include <iostream>
#include <string>
#include <list>
#include <thread>
#include <optional>
#include <mutex>
#include <functional>

using namespace std;

const string DB_SERVER = "sys488";
const string DB_USERNAME = "Minerva_Apps";
const string DB_PASSWORD = "Inf0rmati0n";
const string DB_APP = "Janus";

unsigned int log_id = 0;

class result {
public:
	result(int query, const string& primary_key, const string& change, unsigned int col, optional<string> value1, optional<string> value2) :
		query(query), primary_key(primary_key), change(change), col(col), value1(value1), value2(value2) {
	}

	int query;
	string primary_key;
	string change;
	unsigned int col;
	optional<string> value1, value2;
};

class win_event {
public:
	win_event() {
		h = CreateEvent(nullptr, false, false, nullptr);

		if (!h)
			throw runtime_error("CreateEvent failed (error " + to_string(GetLastError()) + ").");
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
			throw runtime_error("CreateEvent returned " + to_string(ret) + ".");
	}

private:
	HANDLE h;
};

class sql_thread {
public:
	sql_thread(const string_view& query) : finished(false), query(query), tds(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_APP), t([](sql_thread* st) {
			st->run();
		}, this) {
	}

	~sql_thread() {
		t.join();
	}

	void run() {
		try {
			tds::Query sq(tds, query);

			if (!sq.fetch_row())
				throw runtime_error("No results returned.");

			auto num_col = sq.num_columns();

			do {
				unsigned int num_res;

				do {
					lock_guard<mutex> guard(lock);

					num_res = results.size();
				} while (num_res > 100000);

				{
					lock_guard<mutex> guard(lock);

					results.emplace_back();

					auto& v = results.back();

					v.reserve(num_col);

					for (unsigned int i = 0; i < num_col; i++) {
						if (sq[i].is_null())
							v.emplace_back(nullopt);
						else
							v.emplace_back((string)sq[i]);
					}
				}
				
				event.set();
			} while (sq.fetch_row());
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
	tds::Conn tds;
	thread t;
	exception_ptr ex;
	list<vector<optional<string>>> results;
	mutex lock;
	win_event event;
};

static void do_compare(unsigned int num) {
	unsigned int num_rows1 = 0, num_rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0, rows_since_update = 0;
	list<result> res;

	tds::Conn tds(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_APP);

	string q1, q2;
	{
		tds::Query sq(tds, "SELECT query1, query2 FROM Comparer.queries WHERE id=?", num);

		if (!sq.fetch_row())
			throw runtime_error("Unable to find entry in Comparer.queries");

		q1 = sq[0];
		q2 = sq[1];
	}

	bool t1_finished = false, t2_finished = false, t1_done = false, t2_done = false;
	vector<optional<string>> row1, row2;
	list<vector<optional<string>>> rows1, rows2;

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

	t1_fetch();
	t2_fetch();

	{
		tds::Query sq(tds, "INSERT INTO Comparer.log(date, query, success, error) VALUES(GETDATE(), ?, 0, 'Interrupted.'); SELECT SCOPE_IDENTITY()", num);

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
			const string& pk1 = row1[0].value();
			const string& pk2 = row2[0].value();

			if (pk1 == pk2) {
				bool changed = false;

				for (unsigned int i = 1; i < row1.size(); i++) {
					const auto& v1 = row1[i];
					const auto& v2 = row2[i];

					if (!v1.has_value() && v2.has_value()) {
						res.emplace_back(num, pk1, "modified", i + 1, nullopt, v2);
						changed = true;
					} else if (v1.has_value() && !v2.has_value()) {
						res.emplace_back(num, pk1, "modified", i + 1, v1, nullopt);
						changed = true;
					} else if (v1.has_value() && v2.has_value() && v1.value() != v2.value()) {
						res.emplace_back(num, pk1, "modified", i + 1, v1, v2);
						changed = true;
					}
				}

				if (changed)
					changed_rows++;

				num_rows1++;
				num_rows2++;

				t1_fetch();
				t2_fetch();
			} else if (pk1 < pk2) {
				for (unsigned int i = 1; i < row1.size(); i++) {
					const auto& v1 = row1[i];

					if (!v1.has_value())
						res.emplace_back(num, pk1, "removed", i + 1, nullopt, nullopt);
					else
						res.emplace_back(num, pk1, "removed", i + 1, v1.value(), nullopt);
				}

				removed_rows++;
				num_rows1++;

				t1_fetch();
			} else {
				for (unsigned int i = 1; i < row2.size(); i++) {
					const auto& v2 = row2[i];

					if (!v2.has_value())
						res.emplace_back(num, pk2, "added", i + 1, nullopt, nullopt);
					else
						res.emplace_back(num, pk2, "added", i + 1, nullopt, v2.value());
				}

				added_rows++;
				num_rows2++;

				t2_fetch();
			}
		} else if (!t1_finished) {
			const string& pk1 = row1[0].value();

			for (unsigned int i = 1; i < row1.size(); i++) {
				const auto& v1 = row1[i];

				if (!v1.has_value())
					res.emplace_back(num, pk1, "removed", i + 1, nullopt, nullopt);
				else
					res.emplace_back(num, pk1, "removed", i + 1, v1.value(), nullopt);
			}

			removed_rows++;
			num_rows1++;

			t1_fetch();
		} else {
			const string& pk2 = row2[0].value();

			for (unsigned int i = 1; i < row2.size(); i++) {
				const auto& v2 = row2[i];

				if (!v2.has_value())
					res.emplace_back(num, pk2, "added", i + 1, nullopt, nullopt);
				else
					res.emplace_back(num, pk2, "added", i + 1, nullopt, v2.value());
			}

			added_rows++;
			num_rows2++;

			t2_fetch();
		}

		if (res.size() > 10000) { // flush
			vector<vector<optional<string>>> v;

			for (const auto& r : res) {
				v.emplace_back(vector<optional<string>>{to_string(r.query), r.primary_key, r.change, to_string(r.col), r.value1, r.value2});
			}

			tds.bcp("Comparer.results", { "query", "primary_key", "change", "col", "value1", "value2" }, v);

			tds.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE() WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);

			res.clear();
			rows_since_update = 0;
		} else if (rows_since_update > 1000) {
			tds.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE() WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);

			rows_since_update = 0;
		} else
			rows_since_update++;
	}

	if (!res.empty()) {
		vector<vector<optional<string>>> v;

		for (const auto& r : res) {
			v.emplace_back(vector<optional<string>>{to_string(r.query), r.primary_key, r.change, to_string(r.col), r.value1, r.value2});
		}

		tds.bcp("Comparer.results", { "query", "primary_key", "change", "col", "value1", "value2" }, v);
	}

	tds.run("UPDATE Comparer.log SET success=1, rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE(), error=NULL WHERE id=?", num_rows1, num_rows2, changed_rows, added_rows, removed_rows, log_id);
}

int main(int argc, char* argv[]) {
	unsigned int num; 

	if (argc < 2) {
		cerr << "Usage: comparer.exe <query number>" << endl;
		return 1;
	}

	num = stoul(argv[1]);

	try {
		do_compare(num);
	} catch (const exception& e) {
		cerr << "Exception: " << e.what() << endl;

		tds::Conn tds(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_APP);

		if (log_id == 0)
			tds.run("INSERT INTO Comparer.log(query, success, error) VALUES(?, 0, ?)", num, string(e.what()));
		else
			tds.run("UPDATE Comparer.log SET error=?, end_date=GETDATE() WHERE id=?", string(e.what()), log_id);

		return 1;
	}

	return 0;
}
