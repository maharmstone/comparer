#define _CRT_SECURE_NO_WARNINGS

#include <Windows.h>
#include "mercurysql.h"

extern HDBC hdbc;

wstring utf8_to_utf16(const string& s) {
	int len;
	wstring wstr;

	if (s == "")
		return L"";

	len = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), s.length(), NULL, 0);

	if (len == 0)
		return L"";

	wstr = wstring(len, ' ');

	MultiByteToWideChar(CP_UTF8, 0, s.c_str(), s.length(), (WCHAR*)wstr.c_str(), len);

	return wstr;
}

string utf16_to_utf8(const wstring& ws) {
	int len;
	string s;

	if (ws == L"")
		return "";

	len = WideCharToMultiByte(CP_UTF8, 0, ws.c_str(), ws.length(), NULL, 0, NULL, NULL);

	if (len == 0)
		return "";

	s = string(len, ' ');

	WideCharToMultiByte(CP_UTF8, 0, ws.c_str(), ws.length(), (char*)s.c_str(), len, NULL, NULL);

	return s;
}

SQLHStmt::SQLHStmt(HDBC hdbc) {
	SQLRETURN rc;

	rc = SQLAllocStmt(hdbc, &hstmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLAllocStmt", SQL_HANDLE_DBC, hdbc);
}

SQLHStmt::~SQLHStmt() {
	if (hstmt)
		SQLFreeStmt(hstmt, SQL_DROP);
}

void SQLHStmt::SQLPrepare(const string& s) {
	SQLRETURN rc;
	wstring utf16 = utf8_to_utf16(s);

	rc = ::SQLPrepareW(hstmt, (SQLWCHAR*)utf16.c_str(), utf16.length());
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLPrepare", SQL_HANDLE_STMT, hstmt);
}

SQLRETURN SQLHStmt::SQLExecute() {
	SQLRETURN rc;

	rc = ::SQLExecute(hstmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NEED_DATA)
		throw_sql_error("SQLExecute", SQL_HANDLE_STMT, hstmt);

	return rc;
}

void SQLHStmt::SQLBindParameter(SQLUSMALLINT ipar, SQLSMALLINT fParamType, SQLSMALLINT fCType, SQLSMALLINT fSqlType, SQLULEN cbColDef,
	SQLSMALLINT ibScale, SQLPOINTER rgbValue, SQLLEN cbValueMax, SQLLEN * pcbValue) {
	SQLRETURN rc;

	rc= ::SQLBindParameter(hstmt, ipar, fParamType, fCType, fSqlType, cbColDef, ibScale, rgbValue, cbValueMax, pcbValue);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLBindParameter", SQL_HANDLE_STMT, hstmt);
}

SQLRETURN SQLHStmt::SQLPutData(SQLPOINTER DataPtr, SQLLEN StrLen_or_Ind) {
	SQLRETURN rc;

	rc = ::SQLPutData(hstmt, DataPtr, StrLen_or_Ind);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLPutData", SQL_HANDLE_STMT, hstmt);

	return rc;
}

SQLRETURN SQLHStmt::SQLExecDirect(const string& s, const char* func, const char* file, unsigned int line) {
	SQLRETURN rc;
	wstring utf16 = utf8_to_utf16(s);

	rc = ::SQLExecDirectW(hstmt, (SQLWCHAR*)utf16.c_str(), utf16.length());

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NEED_DATA)
		_throw_sql_error("SQLExecDirect", SQL_HANDLE_STMT, hstmt, file, line, func);

	return rc;
}

SQLRETURN SQLHStmt::SQLParamData(SQLPOINTER* ValuePtrPtr) {
	SQLRETURN rc;

	rc = ::SQLParamData(hstmt, ValuePtrPtr);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NEED_DATA)
		throw_sql_error("SQLParamData", SQL_HANDLE_STMT, hstmt);

	return rc;
}

bool SQLHStmt::SQLFetch() {
	SQLRETURN rc;

	rc = ::SQLFetch(hstmt);

	if (rc == SQL_NO_DATA)
		return false;
	else if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
		return true;

	throw_sql_error("SQLFetch", SQL_HANDLE_STMT, hstmt);

	return false;
}

SQLRETURN SQLHStmt::SQLGetData(SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType, SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN* StrLen_or_IndPtr) {
	SQLRETURN rc;

	rc = ::SQLGetData(hstmt, ColumnNumber, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLGetData", SQL_HANDLE_STMT, hstmt);

	return rc;
}

SQLSMALLINT SQLHStmt::SQLNumParams() {
	SQLRETURN rc;
	SQLSMALLINT i;

	rc = ::SQLNumParams(hstmt, &i);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLNumParams", SQL_HANDLE_STMT, hstmt);

	return i;
}

SQLSMALLINT SQLHStmt::SQLNumResultCols() {
	SQLRETURN rc;
	SQLSMALLINT i;

	rc = ::SQLNumResultCols(hstmt, &i);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLNumResultCols", SQL_HANDLE_STMT, hstmt);

	return i;
}

SQLRETURN SQLHStmt::SQLDescribeParam(SQLUSMALLINT ParameterNumber, SQLSMALLINT* DataTypePtr, SQLULEN* ParameterSizePtr, SQLSMALLINT* DecimalDigitsPtr, SQLSMALLINT* NullablePtr) {
	SQLRETURN rc;

	rc = ::SQLDescribeParam(hstmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLDescribeParam", SQL_HANDLE_STMT, hstmt);

	return rc;
}

SQLRETURN SQLHStmt::SQLDescribeCol(SQLUSMALLINT ColumnNumber, SQLCHAR* ColumnName, SQLSMALLINT BufferLength, SQLSMALLINT* NameLengthPtr, SQLSMALLINT* DataTypePtr, SQLULEN* ColumnSizePtr, SQLSMALLINT* DecimalDigitsPtr, SQLSMALLINT* NullablePtr) {
	SQLRETURN rc;

	rc = ::SQLDescribeColA(hstmt, ColumnNumber, ColumnName, BufferLength, NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLDescribeParam", SQL_HANDLE_STMT, hstmt);

	return rc;
}

SQLRETURN SQLHStmt::SQLSetStmtAttr(SQLINTEGER fAttribute, const string& s) {
	SQLRETURN rc;
	wstring utf16 = utf8_to_utf16(s);

	rc = ::SQLSetStmtAttrW(hstmt, fAttribute, (SQLPOINTER)utf16.c_str(), SQL_NTS);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLSetStmtAttr", SQL_HANDLE_STMT, hstmt);

	return rc;
}

void SQLField::reinit(SQLHStmt& hstmt, unsigned int i) {
	SQLINTEGER len;

	null = false;

	if (datatype == SQL_INTEGER || datatype == SQL_BIT || datatype == SQL_NUMERIC || datatype == SQL_BIGINT) {
		hstmt.SQLGetData(i + 1, SQL_C_SBIGINT, &val, sizeof(val), &len);

		if (len == SQL_NULL_DATA)
			null = true;
	} else if (datatype == SQL_TIMESTAMP || datatype == SQL_DATE) {
		hstmt.SQLGetData(i + 1, SQL_C_TIMESTAMP, &ts, sizeof(ts), &len);

		if (len == SQL_NULL_DATA)
			null = true;
	} else if (datatype == SQL_FLOAT || datatype == SQL_DOUBLE) {
		hstmt.SQLGetData(i + 1, SQL_C_DOUBLE, &d, sizeof(d), &len);

		if (len == SQL_NULL_DATA)
			null = true;
	} else if (datatype == SQL_BINARY || datatype == SQL_VARBINARY || datatype == SQL_LONGVARBINARY || datatype == SQL_VARCHAR || datatype == SQL_CHAR) {
		SQLRETURN rc;

		str = "";

		do {
			rc = hstmt.SQLGetData(i + 1, SQL_C_BINARY, (SQLPOINTER)str.c_str(), str.length(), &len);

			if (rc == SQL_SUCCESS) {
				if (len == SQL_NULL_DATA) {
					null = true;
					str = "";
				} else if (len == 0)
					str = "";
			} else if (rc == SQL_SUCCESS_WITH_INFO)
				str.resize(len);
		} while (rc == SQL_SUCCESS_WITH_INFO);
	} else {
		wstring wstr;
		SQLRETURN rc;

		do {
			rc = hstmt.SQLGetData(i + 1, SQL_C_WCHAR, (SQLPOINTER)wstr.c_str(), (wstr.length() + 1) * sizeof(WCHAR), &len);

			if (rc == SQL_SUCCESS || len == 0) {
				if (len == SQL_NULL_DATA) {
					null = true;
					str = "";
				} else if (len > 0)
					str = utf16_to_utf8(wstr);
				else
					str = "";
			} else if (rc == SQL_SUCCESS_WITH_INFO)
				wstr.resize(len / sizeof(WCHAR));
		} while (rc == SQL_SUCCESS_WITH_INFO);
	}
}

SQLField::SQLField(SQLHStmt& hstmt, unsigned int i, bool no_results) {
	SQLSMALLINT namelen;
	SQLUINTEGER colsize;

	hstmt.SQLDescribeCol(i + 1, NULL, 0, &namelen, &datatype, &colsize, &digits, &nullable);

	if (namelen > 0) {
		name = string(namelen + 1, ' ');

		hstmt.SQLDescribeCol(i + 1, (SQLCHAR*)name.c_str(), namelen + 1, &namelen, &datatype, &colsize, &digits, &nullable);

		name = name.substr(0, namelen);
	}

	if (no_results) {
		null = true;
		return;
	}

	reinit(hstmt, i);
}

SQLField::operator string() const {
	if (null)
		return "";

	if (datatype == SQL_INTEGER || datatype == SQL_BIT || datatype == SQL_NUMERIC || datatype == SQL_BIGINT)
		return to_string(val);
	else if (datatype == SQL_TIMESTAMP) {
		char s[20];

		sprintf(s, "%04u-%02u-%02u %02u:%02u:%02u", ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);

		return s;
	} else if (datatype == SQL_DATE) {
		char s[11];

		sprintf(s, "%04u-%02u-%02u", ts.year, ts.month, ts.day);

		return s;
	} else if (datatype == SQL_FLOAT || datatype == SQL_DOUBLE)
		return to_string(d);
	else
		return str;
}

SQLField::operator signed long long() const {
	if (null)
		return 0;

	if (datatype == SQL_INTEGER || datatype == SQL_BIT || datatype == SQL_NUMERIC || datatype == SQL_BIGINT)
		return val;
	else if (datatype == SQL_TIMESTAMP || datatype == SQL_DATE)
		return 0;
	else if (datatype == SQL_FLOAT || datatype == SQL_DOUBLE)
		return (signed long long)d;
	else
		return strtoll(str.c_str(), NULL, 10);
}

SQLField::operator unsigned int() const {
	return (unsigned int)operator signed long long();
}

SQLField::operator int() const {
	return (int)operator signed long long();
}

SQLField::operator bool() const {
	return operator unsigned int() != 0;
}

SQLField::operator double() const {
	if (null)
		return 0.0;

	if (datatype == SQL_INTEGER || datatype == SQL_BIT || datatype == SQL_NUMERIC || datatype == SQL_BIGINT)
		return (double)val;
	else if (datatype == SQL_TIMESTAMP || datatype == SQL_DATE)
		return 0.0;
	else if (datatype == SQL_FLOAT || datatype == SQL_DOUBLE)
		return d;
	else
		return stod(str);
}

bool SQLField::operator==(const string& s) const {
	return operator string() == s;
}

SQLQuery::SQLQuery() : hstmt(hdbc) {
}

SQLQuery::SQLQuery(const string& q, const char* func, const char* file, unsigned int line) : hstmt(hdbc) {
	hstmt.SQLExecDirect(q, func, file, line);
}

SQLQuery::~SQLQuery() {
	for (const auto& buf : bufs) {
		free(buf);
	}
	bufs.clear();
}

bool SQLQuery::fetch_row() {
	bool b = hstmt.SQLFetch();

	if (cols.size() == 0) {
		unsigned int numcols = hstmt.SQLNumResultCols();  

		for (unsigned int i = 0; i < numcols; i++) {
			cols.emplace_back(hstmt, i, !b);
		}
	} else if (b) {
		for (unsigned int i = 0; i < cols.size(); i++) {
			cols[i].reinit(hstmt, i);
		}
	}

	return b;
}

unsigned int SQLQuery::num_cols() {
	return hstmt.SQLNumResultCols();
}

string SQLQuery::col_name(unsigned int i) {
	SQLSMALLINT namelen, datatype, digits, nullable;
	SQLUINTEGER colsize;

	hstmt.SQLDescribeCol(i + 1, NULL, 0, &namelen, &datatype, &colsize, &digits, &nullable);

	if (namelen == 0)
		return "";

	string name(namelen, ' ');

	hstmt.SQLDescribeCol(i + 1, (SQLCHAR*)name.c_str(), namelen + 1, &namelen, &datatype, &colsize, &digits, &nullable);

	return name;
}

SQLSMALLINT SQLQuery::col_type(unsigned int i) {
	SQLSMALLINT namelen, datatype, digits, nullable;
	SQLUINTEGER colsize;

	hstmt.SQLDescribeCol(i + 1, NULL, 0, &namelen, &datatype, &colsize, &digits, &nullable);

	return datatype;
}

void _throw_sql_error(const string& funcname, SQLSMALLINT handle_type, SQLHANDLE handle, const char* filename, unsigned int line, const char* function) {
	char state[SQL_SQLSTATE_SIZE + 1];
	char msg[1000];
	SQLINTEGER error;

	SQLGetDiagRecA(handle_type, handle, 1, (SQLCHAR*)state, &error, (SQLCHAR*)msg, sizeof(msg), NULL);

	throw mercury_exception(funcname + " failed: " + msg, filename, line, function);
}

void SQLInsert(const string& tablename, const vector<string>& np, const vector<vector<nullable<string>>>& vp, const char* func, const char* file, unsigned int line) {
	string nps, vps, q;
	SQLHStmt hstmt(hdbc);
	vector<SQLLEN> lens(np.size());
	vector<SQLSMALLINT> types(np.size());
	vector<bool> nullables(np.size());

	for (const auto& npt : np) {
		if (nps != "")
			nps += ",";

		nps += "[" + npt + "]";

		if (vps != "")
			vps += ",";

		vps += "?";
	}

	q = "INSERT INTO " + tablename + "(" + nps + ") VALUES(" + vps + ")";

	for (unsigned int i = 0; i < np.size(); i++) {
		SQLSMALLINT data_type, decimal_digits, nullable;
		SQLULEN bytes_remaining;

		hstmt.SQLDescribeParam(i + 1, &data_type, &bytes_remaining, &decimal_digits, &nullable);

		if (data_type == SQL_VARBINARY || data_type == SQL_LONGVARBINARY)
			types[i] = SQL_LONGVARBINARY;
		else if (data_type == SQL_VARCHAR || data_type == SQL_LONGVARCHAR)
			types[i] = SQL_LONGVARCHAR;
		else
			types[i] = SQL_VARCHAR;

		nullables[i] = nullable == SQL_NULLABLE;
	}

	for (const auto& v : vp) {
		SQLRETURN rc;

		for (unsigned int i = 0; i < np.size(); i++) {
			if (v[i].is_null() && nullables[i]) {
				lens[i] = SQL_NULL_DATA;

				hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, types[i], string(v[i]).size(), 0, (SQLPOINTER)i, 0, &lens[i]);
			} else {
				lens[i] = SQL_LEN_DATA_AT_EXEC((signed int)string(v[i]).size());

				hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, types[i], string(v[i]).size(), 0, (SQLPOINTER)i, 0, &lens[i]);
			}
		}

		rc = hstmt.SQLExecDirect(q, func, file, line);

		if (rc == SQL_NEED_DATA) {
			SQLPOINTER pParamID;
			unsigned int i;

			rc = hstmt.SQLParamData(&pParamID);

			i = (unsigned int)pParamID;

			do {
				rc = hstmt.SQLPutData((SQLPOINTER)string(v[i]).c_str(), string(v[i]).size());

				rc = hstmt.SQLParamData(&pParamID);

				if (rc == SQL_NEED_DATA)
					i = (unsigned int)pParamID;
			} while (rc == SQL_NEED_DATA);
		}
	}
}

sql_transaction::sql_transaction() {
	SQLRETURN rc = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)false, 0);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLSetConnectAttr", SQL_HANDLE_DBC, hdbc);
}

sql_transaction::~sql_transaction() {
	SQLRETURN rc;

	if (!committed) {
		rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			throw_sql_error("SQLEndTran", SQL_HANDLE_DBC, hdbc);
	}

	rc = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)true, 0);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLSetConnectAttr", SQL_HANDLE_DBC, hdbc);
}

void sql_transaction::commit() {
	SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		throw_sql_error("SQLEndTran", SQL_HANDLE_DBC, hdbc);

	committed = true;
}
