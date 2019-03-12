#include "mercurysql.h"
#include <iostream>
#include <string>

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

static void do_compare(unsigned int num) {
	string q1, q2;
	bool b1, b2;
	unsigned int rows1 = 0, rows2 = 0, changed_rows = 0, added_rows = 0, removed_rows = 0;

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
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, NULL, ?)", num, pk1, "modified", i + 1, (string)sq2.cols[i]);
						changed = true;
					} else if (!sq1.cols[i].null && sq2.cols[i].null) {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, NULL)", num, pk1, "modified", i + 1, (string)sq1.cols[i]);
						changed = true;
					} else if ((string)sq1.cols[i] != (string)sq2.cols[i]) {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, ?)", num, pk1, "modified", i + 1, (string)sq1.cols[i], (string)sq2.cols[i]);
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
					if (sq1.cols[i].null) {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, NULL, NULL)", num, pk1, "removed", i + 1);
					} else {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, NULL)", num, pk1, "removed", i + 1, (string)sq1.cols[i]);
					}
				}

				removed_rows++;
				rows1++;
				b1 = sq1.fetch_row();
			} else {
				for (unsigned int i = 1; i < sq2.cols.size(); i++) {
					if (sq2.cols[i].null) {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, NULL, NULL)", num, pk2, "added", i + 1);
					} else {
						run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, NULL)", num, pk2, "added", i + 1, (string)sq2.cols[i]);
					}
				}

				added_rows++;
				rows2++;
				b2 = sq2.fetch_row();
			}
		} else if (b1) {
			string pk1 = sq1.cols[0];

			for (unsigned int i = 1; i < sq1.cols.size(); i++) {
				if (sq1.cols[i].null) {
					run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, NULL, NULL)", num, pk1, "removed", i + 1);
				} else {
					run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, NULL)", num, pk1, "removed", i + 1, (string)sq1.cols[i]);
				}
			}

			removed_rows++;
			rows1++;
			b1 = sq1.fetch_row();
		} else {
			string pk2 = sq2.cols[0];

			for (unsigned int i = 1; i < sq2.cols.size(); i++) {
				if (sq2.cols[i].null) {
					run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, NULL, NULL)", num, pk2, "added", i + 1);
				} else {
					run_sql("INSERT INTO Comparer.results(query, primary_key, change, col, value1, value2) VALUES(?, ?, ?, ?, ?, NULL)", num, pk2, "added", i + 1, (string)sq2.cols[i]);
				}
			}

			added_rows++;
			rows2++;
			b2 = sq2.fetch_row();
		}
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
