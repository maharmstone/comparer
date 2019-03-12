#pragma once

#include <string>
#include <vector>
#include <Windows.h>
#include <sqlext.h>

using namespace std;

#define SQL_SOPT_SS_QUERYNOTIFICATION_MSGTEXT 1234
#define SQL_SOPT_SS_QUERYNOTIFICATION_OPTIONS 1235
#define SQL_SOPT_SS_QUERYNOTIFICATION_TIMEOUT 1233

#undef SQLSetStmtAttr

wstring utf8_to_utf16(const string& s);
string utf16_to_utf8(const wstring& ws);

extern HDBC hdbc;

class SQLHStmt {
public:
	SQLHStmt(HDBC hdbc);
	~SQLHStmt();
	void SQLPrepare(const string& s);
	SQLRETURN SQLExecute();
	void SQLBindParameter(SQLUSMALLINT ipar, SQLSMALLINT fParamType, SQLSMALLINT fCType, SQLSMALLINT fSqlType, SQLULEN cbColDef,
		SQLSMALLINT ibScale, SQLPOINTER rgbValue, SQLLEN cbValueMax, SQLLEN * pcbValue);
	SQLRETURN SQLPutData(SQLPOINTER DataPtr, SQLLEN StrLen_or_Ind);
	SQLRETURN SQLExecDirect(const string& s, const char* func, const char* file, unsigned int line);
	SQLRETURN SQLParamData(SQLPOINTER* ValuePtrPtr);
	bool SQLFetch();
	SQLRETURN SQLGetData(SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType, SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN* StrLen_or_IndPtr);
	SQLSMALLINT SQLNumParams();
	SQLSMALLINT SQLNumResultCols();
	SQLRETURN SQLDescribeParam(SQLUSMALLINT ParameterNumber, SQLSMALLINT* DataTypePtr, SQLULEN* ParameterSizePtr, SQLSMALLINT* DecimalDigitsPtr, SQLSMALLINT* NullablePtr);
	SQLRETURN SQLDescribeCol(SQLUSMALLINT ColumnNumber, SQLCHAR* ColumnName, SQLSMALLINT BufferLength, SQLSMALLINT* NameLengthPtr, SQLSMALLINT* DataTypePtr, SQLULEN* ColumnSizePtr, SQLSMALLINT* DecimalDigitsPtr, SQLSMALLINT* NullablePtr);
	SQLRETURN SQLSetStmtAttr(SQLINTEGER fAttribute, const string& s);
	SQLRETURN SQLSetStmtAttr(SQLINTEGER fAttribute, unsigned int i);

private:
	HSTMT hstmt = NULL;
};

class json;

template<class T>
class nullable {
public:
	nullable(const T& s) {
		t = s;
		null = false;
	}

	/*NullableString(char* t) {
		s = string(t);
		null = false;
	}*/

	nullable(nullptr_t) {
		null = true;
	}

	nullable() {
		null = true;
	}

	operator T() const {
		if (null)
			return T();
		else
			return t;
	}

	/*bool operator==(const char* s) const {
		return operator string() == s;
	}

	NullableString& NullableString::operator=(const json& j) {
		if (j.type == json_class_type::null)
			null = true;
		else {
			null = false;
			s = j;
		}

		return *this;
	}*/

	bool is_null() const {
		return null;
	}

private:
	T t;
	bool null = false;
};

class SQLField {
public:
	SQLField(SQLHStmt& hstmt, unsigned int i, bool no_results = false);
	void reinit(SQLHStmt& hstmt, unsigned int i);
	operator string() const;
	operator signed long long() const;
	operator unsigned int() const;
	operator int() const;
	//operator nullable<string>() const;
	operator double() const;
	explicit operator bool() const;
	bool operator==(const string& s) const;

	// FIXME - other operators (timestamp, ...?)

	SQLSMALLINT datatype, digits, nullable;
	string name;
	string str;
	signed long long val;
	double d;
	SQL_TIMESTAMP_STRUCT ts;
	bool null = false;

private:
	unsigned int colnum;
	unsigned short reallen;
};

class binary_string {
public:
	binary_string(string s) {
		this->s = s;
	}

	string s;
};

class SQLQuery {
protected:
	void add_params2(unsigned int i, signed long long t) {
		signed long long* buf;

		buf = (signed long long*)malloc(sizeof(t));
		*buf = t;

		bufs.push_back(buf);

		hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_VARCHAR, sizeof(t), 0, (SQLPOINTER)buf, sizeof(t), nullptr);
	}

	void add_params2(unsigned int i, unsigned int t) {
		add_params2(i, (signed long long)t);
	}

	void add_params2(unsigned int i, bool t) {
		add_params2(i, (unsigned int)(t ? 1 : 0));
	}

	void add_params2(unsigned int i, const string& t) {
		wstring w = utf8_to_utf16(t);
		WCHAR* buf;

		buf = (WCHAR*)malloc(2 * (w.size() + 1));
		memcpy(buf, w.c_str(), 2 * w.size());
		buf[w.size()] = 0;

		bufs.push_back(buf);

		hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, w.size() * 2, 0, (SQLPOINTER)buf, w.size() * 2, nullptr);
	}

	void add_params2(unsigned int i, const char* t) {
		add_params2(i, string(t));
	}

	void add_params2(unsigned int i, nullptr_t) {
		SQLLEN* nv = new SQLLEN;

		bufs.push_back(nv);

		*nv = SQL_NULL_DATA;

		hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 0, 0, nullptr, 0, nv);
	}

	void add_params2(unsigned int i, const nullable<string>& ns) {
		if (ns.is_null())
			add_params2(i, nullptr);
		else
			add_params2(i, (string)ns);
	}

	void add_params2(unsigned int i, const nullable<unsigned int>& ns) {
		if (ns.is_null())
			add_params2(i, nullptr);
		else
			add_params2(i, (unsigned int)ns);
	}

	void add_params2(unsigned int i, const binary_string& bs) {
		char* buf;
		SQLLEN* len;

		buf = (char*)malloc(bs.s.size());
		memcpy(buf, bs.s.c_str(), bs.s.size());
		bufs.push_back(buf);

		len = (SQLLEN*)malloc(sizeof(SQLLEN));
		*len = bs.s.size();
		bufs.push_back(len);

		hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_LONGVARBINARY, bs.s.size(), 0, (SQLPOINTER)buf, bs.s.size(), len);
	}

	void add_params2(unsigned int i, double t) {
		double* buf;

		buf = (double*)malloc(sizeof(t));
		*buf = t;

		bufs.push_back(buf);

		hstmt.SQLBindParameter(i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, sizeof(t), 0, (SQLPOINTER)buf, sizeof(t), nullptr);
	}

	void add_params2(unsigned int, const json& j);

	template<typename T>
	void add_params(unsigned int i, T t) {
		add_params2(i, t);
	}

	template<typename T, typename... Args>
	void add_params(unsigned int i, T t, Args... args) {
		add_params(i, t);
		add_params(i + 1, args...);
	}

	SQLQuery();

public:
	SQLQuery(const string& q, const char* func, const char* file, unsigned int line);

	SQLQuery(const string& q, const char* func, const char* file, unsigned int line, const vector<string>& args) : hstmt(hdbc) {
		for (unsigned int i = 0; i < args.size(); i++) {
			add_params(i, args[i]);
		}

		hstmt.SQLExecDirect(q, func, file, line);
	}

	template<typename... Args>
	SQLQuery(const string& q, const char* func, const char* file, unsigned int line, Args... args) : hstmt(hdbc) {
		add_params(0, args...);

		hstmt.SQLExecDirect(q, func, file, line);
	}

	~SQLQuery();
	bool fetch_row();
	unsigned int num_cols();
	string col_name(unsigned int i);
	SQLSMALLINT col_type(unsigned int i);

	SQLHStmt hstmt;
	vector<void*> bufs;
	vector<SQLField> cols;
};

#define SQL(v, s, ...) SQLQuery v(s, __FUNCTION__, __FILE__, __LINE__, __VA_ARGS__)

class SQLQueryNotification : public SQLQuery {
public:
	SQLQueryNotification(const string& q, const string& msgtext, const string& options) {
		hstmt.SQLPrepare(q);

		hstmt.SQLSetStmtAttr(SQL_SOPT_SS_QUERYNOTIFICATION_MSGTEXT, msgtext);
		hstmt.SQLSetStmtAttr(SQL_SOPT_SS_QUERYNOTIFICATION_OPTIONS, options);
		
		hstmt.SQLExecute();
	}

	template<typename... Args>
	SQLQueryNotification(const string& q, const string& msgtext, const string& options, Args... args) {
		hstmt.SQLPrepare(q);

		hstmt.SQLSetStmtAttr(SQL_SOPT_SS_QUERYNOTIFICATION_MSGTEXT, msgtext);
		hstmt.SQLSetStmtAttr(SQL_SOPT_SS_QUERYNOTIFICATION_OPTIONS, options);

		add_params(0, args...);

		hstmt.SQLExecute();
	}
};

class sql_transaction {
public:
	sql_transaction();
	~sql_transaction();
	void commit();

private:
	bool committed = false;
};

class mercury_exception : public runtime_error {
public:
	mercury_exception(const string& arg, const char* file, int line, const char* function = nullptr) : runtime_error(arg) {
		if (function)
			msg = string(function) + ", ";
		else
			msg = "";

		msg += string(file) + ":" + to_string(line) + ": " + arg;
	}

	const char* what() const throw() {
		return msg.c_str();
	}

private:
	string msg;
};

#define throw_exception(s) throw mercury_exception(s, __FILE__, __LINE__)

void _throw_sql_error(const string& funcname, SQLSMALLINT handle_type, SQLHANDLE handle, const char* filename, unsigned int line, const char* function = nullptr);
#define throw_sql_error(funcname, handle_type, handle) _throw_sql_error(funcname, handle_type, handle, __FILE__, __LINE__)

#define run_sql(s, ...) { SQL(sq, s, ##__VA_ARGS__); }

void SQLInsert(const string& tablename, const vector<string>& np, const vector<vector<nullable<string>>>& vp, const char* func, const char* file, unsigned int line);

