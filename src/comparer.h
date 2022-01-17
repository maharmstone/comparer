#pragma once

#include <tdscpp.h>
#include <string>
#include <functional>
#include <list>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <fmt/format.h>
#include <fmt/compile.h>

enum class change {
	modified,
	added,
	removed
};

class _formatted_error : public std::exception {
public:
	template<typename T, typename... Args>
	_formatted_error(const T& s, Args&&... args) : msg(fmt::format(s, std::forward<Args>(args)...)) {
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
	result(int query, const std::string& primary_key, enum change change, unsigned int col,
		   const tds::value& value1, const tds::value& value2, const tds::value& col_name) :
		query(query), primary_key(primary_key), change(change), col(col), value1(value1), value2(value2), col_name(col_name) {
	}

	int query;
	std::string primary_key;
	enum change change;
	unsigned int col;
	tds::value value1, value2, col_name;
};

class sql_thread {
public:
	sql_thread(const std::u16string_view& query, std::unique_ptr<tds::tds>& tds);
	~sql_thread();
	void run() noexcept;
	void wait_for(const std::invocable auto& func);

	bool finished;
	std::u16string query;
	std::unique_ptr<tds::tds> uptds;
	std::thread t;
	std::exception_ptr ex;
	std::vector<std::u16string> names;
	std::list<std::vector<tds::value>> results;
	std::mutex lock;
	std::condition_variable cv;
};
