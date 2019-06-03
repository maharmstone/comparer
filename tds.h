#pragma once

#include <string>
#include <vector>
#include <config.h>
#define TDS_DONT_DEFINE_DEFAULT_FUNCTIONS
extern "C" {
#include <freetds/time.h>
#include <freetds/tds.h>
#include <freetds/convert.h>
#include <freetds/data.h>
}
#include "nullable.h"

typedef int (*tds_handler)(const TDSCONTEXT*, TDSSOCKET*, TDSMESSAGE*);

class TDSProc;
class TDSQuery;
class TDSTrans;

class TDSConn {
public:
	TDSConn(const std::string& server, const std::string& username, const std::string& password, const std::string& app = "",
			tds_handler message_handler = nullptr, tds_handler error_handler = nullptr);
	~TDSConn();
	void bcp(const std::string& table, const std::vector<std::string>& np, const std::vector<std::vector<nullable<std::string>>>& vp);

	friend TDSProc;
	friend TDSQuery;
	friend TDSTrans;

	TDSBCPINFO bcpinfo;

private:
	TDSRET bcp_get_column_data(TDSCOLUMN* bindcol, int offset);

	TDSLOGIN* login = nullptr;
	TDSCONTEXT* context = nullptr;
	TDSSOCKET* sock = nullptr;

	std::vector<std::string> bcp_names;
	const std::vector<std::vector<nullable<std::string>>>* bcp_data;
};

class TDSProc {
public:
	TDSProc(const TDSConn& tds, const std::string& q);
};

class Date {
public:
	Date() : dn(0) { }
	Date(unsigned int year, unsigned int month, unsigned int day);
	Date(int dn) : dn(dn) { }

	unsigned int year() const;
	unsigned int month() const;
	unsigned int day() const;
	std::string to_string() const;

	int dn;

private:
	void calc_date() const;

	mutable unsigned int Y, M, D;
	mutable bool date_calculated = false;
};

class Time {
public:
	Time() : h(0), m(0), s(0) { }
	Time(uint8_t hour, uint8_t minute, uint8_t second) : h(hour), m(minute), s(second) { }

	std::string to_string() const;

	uint8_t h, m, s;
};

class TDSField {
public:
	TDSField(const std::string& name, TDS_SERVER_TYPE type) : name(name), type(type) {
	}

	operator std::string() const;
	explicit operator int64_t() const;
	explicit operator double() const;

	explicit operator int32_t() const {
		return (int32_t)operator int64_t();
	}

	explicit operator uint32_t() const {
		return (uint32_t)operator int64_t();
	}

	explicit operator uint16_t() const {
		return (uint16_t)operator int64_t();
	}

	operator nullable<std::string>() const {
		if (null)
			return nullptr;
		else
			return operator std::string();
	}

	bool operator==(const std::string& s) const {
		return std::string() == s;
	}

	bool is_null() const {
		return null;
	}

	std::string name;
	TDS_SERVER_TYPE type;

	friend TDSQuery;

private:
	std::string strval;
	int64_t intval;
	Date date;
	Time time;
	double doubval;
	bool null;
};

class TDSParam {
public:
	TDSParam(const std::string& s) : null(false), s(s) {
	}

	TDSParam(nullptr_t) : null(true) {
	}

	bool null;
	std::string s;
};

class TDSQuery {
public:
	TDSQuery(const TDSConn& tds, const std::string& q) : tds(tds) {
		start_query(q);
		end_query();
	}

	template<typename... Args>
	TDSQuery(const TDSConn& tds, const std::string& q, Args... args) : tds(tds) {
		start_query(q);
		add_param(0, args...);
		end_query();
	}

	~TDSQuery();

	bool fetch_row();

	const TDSField& operator[](unsigned int i) const {
		return cols.at(i);
	}

	size_t num_columns() const {
		return cols.size();
	}

private:
	void start_query(const std::string& q);
	void add_param2(unsigned int i, const std::string& param);
	void add_param2(unsigned int i, int32_t v);
	void add_param2(unsigned int, const binary_string& bs);
	void add_param2(unsigned int, const TDSParam& p);

	template<typename T>
	void add_param(unsigned int i, const T& param) {
		add_param2(i, param);
	}

	template<typename T, typename... Args>
	void add_param(unsigned int i, const T& param, Args... args) {
		add_param2(i, param);
		add_param(i + 1, args...);
	}

	void end_query();

	std::vector<TDSField> cols;
	const TDSConn& tds;
	TDSDYNAMIC* dyn = nullptr;
	tds_handler old_msg_handler = nullptr, old_err_handler = nullptr;
};

class TDSTrans {
public:
	TDSTrans(const TDSConn& tds);
	~TDSTrans();
	void commit();

private:
	TDSSOCKET* sock = nullptr;
	bool committed = false;
};

TDSParam null_string(const std::string& s);

static void tds_run(const TDSConn& tds, const std::string& s) {
	TDSQuery q(tds, s);
}

template<typename... Args>
static void tds_run(const TDSConn& tds, const std::string& s, Args... args) {
	TDSQuery q(tds, s, args...);
}
