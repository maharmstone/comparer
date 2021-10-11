#pragma once

#include <windows.h>
#include <tdscpp.h>
#include <string>
#include <functional>
#include <list>
#include <thread>
#include <memory>
#include <mutex>
#include <fmt/format.h>
#include <fmt/compile.h>

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

class handle_closer {
public:
	typedef HANDLE pointer;

	void operator()(HANDLE h) {
		if (h == INVALID_HANDLE_VALUE)
			return;

		CloseHandle(h);
	}
};

typedef std::unique_ptr<HANDLE, handle_closer> unique_handle;

class win_event {
public:
	win_event();
	void set() noexcept;
	void wait();

private:
	unique_handle h;
};

class sql_thread {
public:
	sql_thread(const std::string_view& server, const std::u16string_view& query);
	~sql_thread();
	void run() noexcept;
	void wait_for(const std::invocable auto& func);

	bool finished;
	std::u16string query;
	tds::tds tds;
	std::thread t;
	std::exception_ptr ex;
	std::vector<std::u16string> names;
	std::list<std::vector<tds::value>> results;
	std::mutex lock;
	win_event event;
};
