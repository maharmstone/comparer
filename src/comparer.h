#pragma once

#ifdef _WIN32
#include <windows.h>
#endif

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

class sql_thread {
public:
	sql_thread(std::u16string_view query, std::unique_ptr<tds::tds>& tds);
	~sql_thread();
	void run(std::stop_token) noexcept;
	void wait_for(const std::invocable auto& func);

	bool finished;
	std::u16string query;
	std::unique_ptr<tds::tds> uptds;
	std::exception_ptr ex;
	std::vector<tds::column> cols;
	std::list<std::vector<std::pair<tds::value_data_t, bool>>> results;
	std::mutex lock;
	std::condition_variable cv;
	std::jthread t;
};

#ifdef _WIN32

class handle_closer {
public:
	typedef HANDLE pointer;

	void operator()(HANDLE h) {
		if (h == INVALID_HANDLE_VALUE)
			return;

		CloseHandle(h);
	}
};

using unique_handle = std::unique_ptr<HANDLE, handle_closer>;

#else

class unique_handle {
public:
	unique_handle() : fd(0) {
	}

	explicit unique_handle(int fd) : fd(fd) {
	}

	unique_handle(unique_handle&& that) noexcept {
		fd = that.fd;
		that.fd = 0;
	}

	unique_handle(const unique_handle&) = delete;
	unique_handle& operator=(const unique_handle&) = delete;

	unique_handle& operator=(unique_handle&& that) noexcept {
		if (fd > 0)
			close(fd);

		fd = that.fd;
		that.fd = 0;

		return *this;
	}

	~unique_handle() {
		if (fd <= 0)
			return;

		close(fd);
	}

	explicit operator bool() const noexcept {
		return fd != 0;
	}

	void reset(int new_fd = 0) noexcept {
		if (fd > 0)
			close(fd);

		fd = new_fd;
	}

	int get() const noexcept {
		return fd;
	}

private:
	int fd;
};

#endif

#ifdef _WIN32

class last_error : public std::exception {
public:
	last_error(std::string_view function, DWORD le) {
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

#else

class errno_error : public std::exception {
public:
	errno_error(std::string_view function, int en);

	const char* what() const noexcept {
		return msg.c_str();
	}

private:
	std::string msg;
};

#endif

class bcp_thread {
public:
	bcp_thread() {
		t = std::jthread([this](std::stop_token stop) noexcept {
				this->run(stop);
		});
	}

	std::list<std::vector<tds::value>> res;
	std::condition_variable_any cv;
	std::mutex lock;
	std::exception_ptr exc;
	std::jthread t;

private:
	void run(std::stop_token stop) noexcept;
};
