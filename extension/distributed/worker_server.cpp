//===----------------------------------------------------------------------===//
//                         DuckDB - Distributed Extension
//
// worker_server.cpp
//
// TCP server implementation. Each worker DuckDB instance runs one of these
// servers. Coordinators connect via TCP and send queries using the distributed
// wire protocol; the server executes them with a local Connection and streams
// results back as TSV.
//===----------------------------------------------------------------------===//

#include "worker_server.hpp"
#include "distributed_context.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Construction / destruction
//===--------------------------------------------------------------------===//

WorkerServer::WorkerServer(DatabaseInstance &db, int32_t port) : db(db), port(port), server_fd(-1) {
}

WorkerServer::~WorkerServer() {
	Stop();
}

//===--------------------------------------------------------------------===//
// Start / Stop
//===--------------------------------------------------------------------===//

void WorkerServer::Start() {
	if (running.load()) {
		throw std::runtime_error("WorkerServer is already running on port " + to_string(port));
	}

	// Ignore SIGPIPE so that writing to a closed socket returns EPIPE instead
	// of killing the process.
	signal(SIGPIPE, SIG_IGN);

	// Create TCP server socket.
	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd < 0) {
		throw std::runtime_error(string("socket() failed: ") + strerror(errno));
	}

	// Allow fast re-bind after restart.
	int opt = 1;
	setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	sockaddr_in addr {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(static_cast<uint16_t>(port));

	if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
		close(server_fd);
		server_fd = -1;
		throw std::runtime_error(string("bind() failed on port ") + to_string(port) + ": " + strerror(errno));
	}

	if (listen(server_fd, /*backlog=*/64) < 0) {
		close(server_fd);
		server_fd = -1;
		throw std::runtime_error(string("listen() failed: ") + strerror(errno));
	}

	running.store(true);
	accept_thread = std::thread(&WorkerServer::AcceptLoop, this);
}

void WorkerServer::Stop() {
	if (!running.exchange(false)) {
		return; // already stopped
	}

	// shutdown() is required to reliably interrupt a blocking accept() in the
	// accept thread on Linux.  A plain close() is not guaranteed to unblock
	// another thread that is inside accept() on the same fd.
	if (server_fd >= 0) {
		shutdown(server_fd, SHUT_RDWR);
		close(server_fd);
		server_fd = -1;
	}

	if (accept_thread.joinable()) {
		accept_thread.join();
	}

	// Join any still-running connection threads.
	std::lock_guard<std::mutex> lock(conn_threads_mutex);
	for (auto &t : conn_threads) {
		if (t.joinable()) {
			t.join();
		}
	}
	conn_threads.clear();
}

//===--------------------------------------------------------------------===//
// Accept loop
//===--------------------------------------------------------------------===//

void WorkerServer::AcceptLoop() {
	while (running.load()) {
		sockaddr_in client_addr {};
		socklen_t client_len = sizeof(client_addr);
		int client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);

		if (client_fd < 0) {
			if (!running.load()) {
				break; // server is shutting down
			}
			// Transient error – keep accepting.
			continue;
		}

		// Spawn a thread for this connection.
		std::lock_guard<std::mutex> lock(conn_threads_mutex);
		conn_threads.emplace_back([this, client_fd]() { HandleClient(client_fd); });
	}
}

//===--------------------------------------------------------------------===//
// Per-connection handler
//===--------------------------------------------------------------------===//

void WorkerServer::HandleClient(int client_fd) {
	try {
		string cmd = ReadLine(client_fd);

		if (cmd == "PING") {
			SendLine(client_fd, "PONG");
			close(client_fd);
			return;
		}

		if (cmd != "QUERY") {
			SendLine(client_fd, "ERR");
			SendLine(client_fd, "Unknown command: " + cmd);
			close(client_fd);
			return;
		}

		// Read SQL length and then the SQL text.
		string len_str = ReadLine(client_fd);
		int sql_len = std::stoi(len_str);
		if (sql_len <= 0 || sql_len > 64 * 1024 * 1024) {
			SendLine(client_fd, "ERR");
			SendLine(client_fd, "Invalid SQL length: " + len_str);
			close(client_fd);
			return;
		}
		string sql = ReadExact(client_fd, sql_len);
		ReadLine(client_fd); // consume the trailing '\n'

		// Execute via a local DuckDB connection.
		DuckDB duckdb_wrapper(db);
		Connection conn(duckdb_wrapper);
		auto result = conn.Query(sql);

		if (result->HasError()) {
			SendLine(client_fd, "ERR");
			SendLine(client_fd, result->GetError());
			close(client_fd);
			return;
		}

		// Send success header.
		SendLine(client_fd, "OK");

		idx_t num_cols = result->types.size();
		SendLine(client_fd, to_string(num_cols));

		// Send column names and types.
		for (idx_t col = 0; col < num_cols; col++) {
			SendLine(client_fd, result->names[col] + "\t" + result->types[col].ToString());
		}

		SendLine(client_fd, "DATA");

		// Stream result rows as TSV using DataChunk Fetch() iteration.
		unique_ptr<DataChunk> chunk;
		while ((chunk = result->Fetch()) && chunk->size() > 0) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				string row_line;
				for (idx_t col = 0; col < num_cols; col++) {
					if (col > 0) {
						row_line += '\t';
					}
					auto val = chunk->GetValue(col, row);
					if (val.IsNull()) {
						row_line += "\\N";
					} else {
						row_line += EscapeValue(val.ToString());
					}
				}
				SendLine(client_fd, row_line);
			}
		}

		SendLine(client_fd, "DONE");

	} catch (std::exception &e) {
		// Best-effort: try to inform the client about the error.
		try {
			SendLine(client_fd, "ERR");
			SendLine(client_fd, string(e.what()));
		} catch (...) {
		}
	} catch (...) {
	}

	close(client_fd);
}

//===--------------------------------------------------------------------===//
// Socket I/O helpers
//===--------------------------------------------------------------------===//

string WorkerServer::ReadLine(int fd) {
	string line;
	char ch;
	while (true) {
		ssize_t n = recv(fd, &ch, 1, 0);
		if (n <= 0) {
			throw std::runtime_error("Connection closed while reading line");
		}
		if (ch == '\n') {
			break;
		}
		line += ch;
	}
	return line;
}

string WorkerServer::ReadExact(int fd, int n) {
	string buf(n, '\0');
	int total = 0;
	while (total < n) {
		ssize_t r = recv(fd, &buf[total], n - total, 0);
		if (r <= 0) {
			throw std::runtime_error("Connection closed while reading data");
		}
		total += static_cast<int>(r);
	}
	return buf;
}

void WorkerServer::SendLine(int fd, const string &line) {
	string msg = line + "\n";
	const char *ptr = msg.c_str();
	int remaining = static_cast<int>(msg.size());
	while (remaining > 0) {
		ssize_t s = send(fd, ptr, remaining, MSG_NOSIGNAL);
		if (s < 0) {
			throw std::runtime_error(string("send() failed: ") + strerror(errno));
		}
		ptr += s;
		remaining -= static_cast<int>(s);
	}
}

} // namespace duckdb
