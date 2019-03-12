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

int main() {
	try {
		db_connect();

		// FIXME
	} catch (const exception& e) {
		cerr << "Exception: " << e.what() << endl;
		return 1;
	}

	return 0;
}
