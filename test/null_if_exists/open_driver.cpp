// Opens Azure files with FILE_FLAGS_EXCLUSIVE_CREATE (and NULL_IF_EXISTS) through DuckDB's file system. SQL sets
// NULL_IF_EXISTS nowhere (unity_catalog sets it from C++), so the tests open files here. Built and run by
// check_null_if_exists.py against build/release/src/libduckdb.so.
//
// Usage: open_driver <azure.duckdb_extension> <setup.sql> <op> <url> [arg]
//   nie  <url> <content>  EXCLUSIVE_CREATE | NULL_IF_EXISTS; prints "created" (and writes content) or "null"
//   excl <url> <content>  EXCLUSIVE_CREATE only; prints "created" or
//                         "error: <message> [errno=<extra_info errno, or none> type=<exception type>]"
//   race <url> <n>        n threads run "nie" on url at once, content "writer-<i>";
//                         prints "winners=<k> errors=<e> winner=writer-<i>"
//   cat  <url>            prints "size=<n> content=<bytes>", or "missing"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

using namespace duckdb;

static const FileOpenFlags CREATE_FLAGS =
    FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;

static void Check(unique_ptr<QueryResult> res) {
	if (res->HasError()) {
		throw std::runtime_error(res->GetError());
	}
}

// The first connection runs all of setup; later ones only its SET lines (LOAD and CREATE SECRET are per database)
static unique_ptr<Connection> Connect(DuckDB &db, const string &setup, bool first = true) {
	auto con = make_uniq<Connection>(db);
	std::stringstream ss(setup);
	string line;
	while (std::getline(ss, line)) {
		if (!line.empty() && (first || StringUtil::StartsWith(line, "SET "))) {
			Check(con->Query(line));
		}
	}
	return con;
}

// Returns true if the file was created (and content written), false if OpenFile gave nullptr
static bool CreateWith(Connection &con, const string &url, FileOpenFlags flags, const string &content) {
	bool created = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &fs = FileSystem::GetFileSystem(*con.context);
		auto handle = fs.OpenFile(url, flags);
		if (!handle) {
			return;
		}
		created = true;
		handle->Write((void *)content.data(), content.size());
		handle->Sync();
		handle->Close();
	});
	return created;
}

int main(int argc, char **argv) {
	if (argc < 5) {
		std::cerr << "usage: open_driver <extension> <setup.sql> <op> <url> [arg]\n";
		return 2;
	}
	string extension = argv[1], op = argv[3], url = argv[4], arg = argc > 5 ? argv[5] : "";
	std::ifstream setup_file(argv[2]);
	std::stringstream setup_buf;
	setup_buf << setup_file.rdbuf();
	string setup = "LOAD '" + extension + "';\n" + setup_buf.str();

	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	config.SetOptionByName("autoload_known_extensions", Value::BOOLEAN(false));
	DuckDB db(nullptr, &config);
	auto con = Connect(db, setup);

	try {
		if (op == "nie") {
			auto created = CreateWith(*con, url, CREATE_FLAGS | FileFlags::FILE_FLAGS_NULL_IF_EXISTS, arg);
			std::cout << (created ? "created" : "null") << std::endl;
		} else if (op == "excl") {
			try {
				auto created = CreateWith(*con, url, CREATE_FLAGS, arg);
				std::cout << (created ? "created" : "null") << std::endl;
			} catch (std::exception &e) {
				ErrorData error(e);
				auto &info = error.ExtraInfo();
				auto it = info.find("errno");
				std::cout << "error: " << error.RawMessage() << " [errno=" << (it == info.end() ? "none" : it->second)
				          << " type=" << Exception::ExceptionTypeToString(error.Type()) << "]" << std::endl;
			}
		} else if (op == "race") {
			int n = std::stoi(arg);
			vector<unique_ptr<Connection>> cons;
			for (int i = 0; i < n; i++) {
				cons.push_back(Connect(db, setup, false));
			}
			std::mutex mu;
			std::condition_variable cv;
			int ready = 0;
			std::atomic<int> winners {0};
			std::atomic<int> errors {0};
			std::atomic<int> winner {-1};
			vector<std::thread> threads;
			for (int i = 0; i < n; i++) {
				threads.emplace_back([&, i]() {
					{
						std::unique_lock<std::mutex> lock(mu);
						ready++;
						cv.notify_all();
						cv.wait(lock, [&]() { return ready == n; });
					}
					try {
						if (CreateWith(*cons[i], url, CREATE_FLAGS | FileFlags::FILE_FLAGS_NULL_IF_EXISTS,
						               "writer-" + std::to_string(i))) {
							winners++;
							winner = i;
						}
					} catch (std::exception &e) {
						ErrorData error(e);
						std::lock_guard<std::mutex> lock(mu);
						std::cerr << "writer-" << i << ": " << error.RawMessage() << std::endl;
						errors++;
					}
				});
			}
			for (auto &t : threads) {
				t.join();
			}
			std::cout << "winners=" << winners.load() << " errors=" << errors.load() << " winner=writer-"
			          << winner.load() << std::endl;
		} else if (op == "cat") {
			con->context->RunFunctionInTransaction([&]() {
				auto &fs = FileSystem::GetFileSystem(*con->context);
				auto handle = fs.OpenFile(url, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
				if (!handle) {
					std::cout << "missing" << std::endl;
					return;
				}
				auto size = handle->GetFileSize();
				string buf(size, '\0');
				if (size > 0) {
					handle->Read((void *)buf.data(), size, 0);
				}
				std::cout << "size=" << size << " content=" << buf << std::endl;
			});
		} else {
			std::cerr << "unknown op " << op << std::endl;
			return 2;
		}
	} catch (std::exception &e) {
		ErrorData error(e);
		std::cout << "error: " << error.RawMessage() << std::endl;
		return 1;
	}
	return 0;
}
