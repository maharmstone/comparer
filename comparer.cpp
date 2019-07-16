#include <tdscpp.h>
#include <iostream>
#include <string>
#include <list>

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

static void do_compare(unsigned int num) {
	bool b1, b2;
	unsigned int rows1 = 0, rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0;
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

	{
		tds::Query sq1(tds, q1);

		tds::Conn tds2(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_APP);

		tds::Query sq2(tds2, q2);

		b1 = sq1.fetch_row();
		b2 = sq2.fetch_row();

		while (b1 || b2) {
			if (b1 && b2) {
				string pk1 = sq1[0], pk2 = sq2[0];

				if (pk1 == pk2) {
					bool changed = false;

					for (unsigned int i = 1; i < sq1.num_columns(); i++) {
						const auto& v1 = sq1[i];
						const auto& v2 = sq2[i];

						if (v1.is_null() && !v2.is_null()) {
							res.emplace_back(num, pk1, "modified", i + 1, nullopt, (string)v2);
							changed = true;
						} else if (!v1.is_null() && v2.is_null()) {
							res.emplace_back(num, pk1, "modified", i + 1, (string)v1, nullopt);
							changed = true;
						} else if ((string)v1 != (string)v2) {
							res.emplace_back(num, pk1, "modified", i + 1, (string)v1, (string)v2);
							changed = true;
						}
					}

					if (changed)
						changed_rows++;

					rows1++;
					rows2++;
					b1 = sq1.fetch_row();
					b2 = sq2.fetch_row();
				} else if (pk1 < pk2) {
					for (unsigned int i = 1; i < sq1.num_columns(); i++) {
						if (sq1[i].is_null())
							res.emplace_back(num, pk1, "removed", i + 1, nullopt, nullopt);
						else
							res.emplace_back(num, pk1, "removed", i + 1, (string)sq1[i], nullopt);
					}

					removed_rows++;
					rows1++;
					b1 = sq1.fetch_row();
				} else {
					for (unsigned int i = 1; i < sq2.num_columns(); i++) {
						if (sq2[i].is_null())
							res.emplace_back(num, pk2, "added", i + 1, nullopt, nullopt);
						else
							res.emplace_back(num, pk2, "added", i + 1, nullopt, (string)sq2[i]);
					}

					added_rows++;
					rows2++;
					b2 = sq2.fetch_row();
				}
			} else if (b1) {
				string pk1 = sq1[0];

				for (unsigned int i = 1; i < sq1.num_columns(); i++) {
					if (sq1[i].is_null())
						res.emplace_back(num, pk1, "removed", i + 1, nullopt, nullopt);
					else
						res.emplace_back(num, pk1, "removed", i + 1, (string)sq1[i], nullopt);
				}

				removed_rows++;
				rows1++;
				b1 = sq1.fetch_row();
			} else {
				string pk2 = sq2[0];

				for (unsigned int i = 1; i < sq2.num_columns(); i++) {
					if (sq2[i].is_null())
						res.emplace_back(num, pk2, "added", i + 1, nullopt, nullopt);
					else
						res.emplace_back(num, pk2, "added", i + 1, nullopt, (string)sq2[i]);
				}

				added_rows++;
				rows2++;
				b2 = sq2.fetch_row();
			}

			if (res.size() > 10000) { // flush
				vector<vector<optional<string>>> v;

				for (const auto& r : res) {
					v.emplace_back(vector<optional<string>>{to_string(r.query), r.primary_key, r.change, to_string(r.col), r.value1, r.value2});
				}

				tds::Conn tds3(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_APP);
				tds3.bcp("Comparer.results", { "query", "primary_key", "change", "col", "value1", "value2" }, v);

				tds3.run("UPDATE Comparer.log SET rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE() WHERE id=?", rows1, rows2, changed_rows, added_rows, removed_rows, log_id);

				res.clear();
			}
		}
	}

	if (!res.empty()) {
		vector<vector<optional<string>>> v;

		for (const auto& r : res) {
			v.emplace_back(vector<optional<string>>{to_string(r.query), r.primary_key, r.change, to_string(r.col), r.value1, r.value2});
		}

		tds.bcp("Comparer.results", { "query", "primary_key", "change", "col", "value1", "value2" }, v);
	}

	tds.run("UPDATE Comparer.log SET success=1, rows1=?, rows2=?, changed_rows=?, added_rows=?, removed_rows=?, end_date=GETDATE(), error=NULL WHERE id=?", rows1, rows2, changed_rows, added_rows, removed_rows, log_id);
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
