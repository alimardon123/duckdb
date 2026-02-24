//===----------------------------------------------------------------------===//
//                         DuckDB
//
// worker_server.hpp
//
// A lightweight TCP server that turns a DuckDB instance into a query worker.
// Clients (coordinators) connect, send a SQL query in the distributed wire
// protocol, and receive results serialised as TSV.
//
// Wire protocol (text, UTF-8):
//
//   Request
//   -------
//   "QUERY\n"
//   "<sql_byte_count_decimal>\n"
//   <sql_bytes> "\n"
//
//   Success response
//   ----------------
//   "OK\n"
//   "<num_cols_decimal>\n"
//   "<col1_name>\t<col1_type>\n"   (repeated num_cols times)
//   "DATA\n"
//   "<val1>\t<val2>\t...\n"        (one line per row; \N = NULL;
//                                   backslash-escaped tabs/newlines/backslashes)
//   "DONE\n"
//
//   Error response
//   --------------
//   "ERR\n"
//   "<error_message>\n"
//
//   Health-check
//   ------------
//   Request:  "PING\n"
//   Response: "PONG\n"
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

namespace duckdb {

class WorkerServer {
public:
	explicit WorkerServer(DatabaseInstance &db, int32_t port);
	~WorkerServer();

	//! Start the background accept thread. Throws on bind/listen failure.
	void Start();

	//! Signal the server to stop and wait for the accept thread to exit.
	void Stop();

	bool IsRunning() const {
		return running.load();
	}

	int32_t GetPort() const {
		return port;
	}

private:
	//! Main accept loop (runs in accept_thread).
	void AcceptLoop();

	//! Handle a single connected client (runs in a per-connection thread).
	void HandleClient(int client_fd);

	//! Read a '\n'-terminated line from the socket (newline is stripped).
	static string ReadLine(int fd);

	//! Read exactly n bytes from the socket.
	static string ReadExact(int fd, int n);

	//! Write a string followed by '\n' to the socket.
	static void SendLine(int fd, const string &line);

	DatabaseInstance &db;
	int32_t port;
	int server_fd;
	std::atomic<bool> running {false};
	std::thread accept_thread;

	//! Tracks per-connection threads so we can join them on Stop().
	std::mutex conn_threads_mutex;
	std::vector<std::thread> conn_threads;
};

} // namespace duckdb
