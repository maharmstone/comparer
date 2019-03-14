#include "mercurysql.h"
#include <iostream>
#include <string>
#include <list>

using namespace std;

HDBC hdbc = nullptr;
HENV henv;

const string DB_USERNAME = "Minerva_Apps";
const string DB_PASSWORD = "Inf0rmati0n";
const string CONNEXION_STRING = "DRIVER=SQL Server Native Client 11.0;SERVER=sys488;UID=" + DB_USERNAME + ";PWD=" + DB_PASSWORD + ";APP=Comparer;MARS_Connection=yes";

void db_connect() {
	SQLRETURN rc;

	if (hdbc)
		return;

	SQLAllocEnv(&henv);
	SQLAllocConnect(henv, &hdbc);

	try {
		rc = SQLSetConnectAttr(hdbc, SQL_COPT_SS_BCP, (void*)SQL_BCP_ON, SQL_IS_INTEGER);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			throw_sql_error("SQLSetConnectAttr", SQL_HANDLE_DBC, hdbc);

		rc = SQLDriverConnectA(hdbc, NULL, (unsigned char*)CONNEXION_STRING.c_str(), (SQLSMALLINT)CONNEXION_STRING.length(), NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			throw_sql_error("SQLDriverConnect", SQL_HANDLE_DBC, hdbc);
	} catch (...) {
		SQLFreeConnect(henv);
		SQLFreeEnv(henv);
		SQLFreeConnect(hdbc);

		hdbc = nullptr;
		throw;
	}
}

class result {
public:
	result(int query, const string& primary_key, const string& change, unsigned int col, nullable<string> value1, nullable<string> value2) :
		query(query), primary_key(primary_key), change(change), col(col), value1(value1), value2(value2) {
	}

	int query;
	string primary_key;
	string change;
	unsigned int col;
	nullable<string> value1, value2;
};

static void do_compare(unsigned int num) {
	string q1, q2;
	bool b1, b2;
	unsigned int rows1 = 0, rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0;
	list<result> res;

	{
		SQL(sq, "SELECT query1, query2 FROM Comparer.queries WHERE id=?", num);

		if (!sq.fetch_row())
			throw runtime_error("Unable to find entry in Comparer.queries");

		q1 = sq.cols[0];
		q2 = sq.cols[1];
	}

	{
		run_sql("DELETE FROM Comparer.results WHERE query=?", num);
	}

	SQL(sq1, q1);
	SQL(sq2, q2);

	b1 = sq1.fetch_row();
	b2 = sq2.fetch_row();

	while (b1 || b2) {
		if (b1 && b2) {
			string pk1 = sq1.cols[0], pk2 = sq2.cols[0];

			if (pk1 == pk2) {
				bool changed = false;

				for (unsigned int i = 1; i < sq1.cols.size(); i++) {
					if (sq1.cols[i].null && !sq2.cols[i].null) {
						res.emplace_back(num, pk1, "modified", i + 1, nullptr, (string)sq2.cols[i]);
						changed = true;
					} else if (!sq1.cols[i].null && sq2.cols[i].null) {
						res.emplace_back(num, pk1, "modified", i + 1, (string)sq1.cols[i], nullptr);
						changed = true;
					} else if ((string)sq1.cols[i] != (string)sq2.cols[i]) {
						res.emplace_back(num, pk1, "modified", i + 1, (string)sq1.cols[i], (string)sq2.cols[i]);
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
				for (unsigned int i = 1; i < sq1.cols.size(); i++) {
					if (sq1.cols[i].null)
						res.emplace_back(num, pk1, "removed", i + 1, nullptr, nullptr);
					else
						res.emplace_back(num, pk1, "removed", i + 1, (string)sq1.cols[i], nullptr);
				}

				removed_rows++;
				rows1++;
				b1 = sq1.fetch_row();
			} else {
				for (unsigned int i = 1; i < sq2.cols.size(); i++) {
					if (sq2.cols[i].null)
						res.emplace_back(num, pk2, "added", i + 1, nullptr, nullptr);
					else
						res.emplace_back(num, pk2, "added", i + 1, nullptr, (string)sq2.cols[i]);
				}

				added_rows++;
				rows2++;
				b2 = sq2.fetch_row();
			}
		} else if (b1) {
			string pk1 = sq1.cols[0];

			for (unsigned int i = 1; i < sq1.cols.size(); i++) {
				if (sq1.cols[i].null)
					res.emplace_back(num, pk1, "removed", i + 1, nullptr, nullptr);
				else
					res.emplace_back(num, pk1, "removed", i + 1, (string)sq1.cols[i], nullptr);
			}

			removed_rows++;
			rows1++;
			b1 = sq1.fetch_row();
		} else {
			string pk2 = sq2.cols[0];

			for (unsigned int i = 1; i < sq2.cols.size(); i++) {
				if (sq2.cols[i].null)
					res.emplace_back(num, pk2, "added", i + 1, nullptr, nullptr);
				else
					res.emplace_back(num, pk2, "added", i + 1, nullptr, (string)sq2.cols[i]);
			}

			added_rows++;
			rows2++;
			b2 = sq2.fetch_row();
		}
	}

	if (!res.empty()) {
		forward_list<list<nullable<string>>> v;

		for (const auto& r : res) {
			v.emplace_front(list<nullable<string>>{to_string(r.query), r.primary_key, r.change, to_string(r.col), r.value1, r.value2});
		}

		SQLInsert_Batch("", "Comparer.results", { "query", "primary_key", "change", "col", "value1", "value2" }, v);
	}

	run_sql("INSERT INTO Comparer.log(query, success, rows1, rows2, changed_rows, added_rows, removed_rows) VALUES(?, 1, ?, ?, ?, ?, ?)", num, rows1, rows2, changed_rows, added_rows, removed_rows);
}

int main(int argc, char* argv[]) {
	unsigned int num; 

	if (argc < 2) {
		cerr << "Usage: comparer.exe query_number" << endl;
		return 1;
	}

	try {
		db_connect();
	} catch (const exception& e) {
		cerr << "Exception: " << e.what() << endl;
		return 1;
	}

	try {
		num = stoul(argv[1]);

		do_compare(num);
	} catch (const exception& e) {
		cerr << "Exception: " << e.what() << endl;
		run_sql("INSERT INTO Comparer.log(query, success, error) VALUES(?, 0, ?)", num, e.what());

		return 1;
	}

	return 0;
}
