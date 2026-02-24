//===----------------------------------------------------------------------===//
//                         DuckDB - Distributed Extension
//
// distributed_context.cpp
//
// Manages the cluster node registry (add/remove/list nodes) and implements
// the TCP client that coordinators use to send queries to worker nodes.
//===----------------------------------------------------------------------===//

#include "distributed_context.hpp"
#include "worker_server.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Singleton
//===--------------------------------------------------------------------===//

DistributedContext &DistributedContext::Get() {
	static DistributedContext instance;
	return instance;
}

//===--------------------------------------------------------------------===//
// Node registry
//===--------------------------------------------------------------------===//

void DistributedContext::AddNode(const string &host, int32_t port) {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	for (auto &n : nodes) {
		if (n.host == host && n.port == port) {
			return; // already registered
		}
	}
	nodes.push_back({host, port});
}

void DistributedContext::RemoveNode(const string &host, int32_t port) {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	nodes.erase(std::remove_if(nodes.begin(), nodes.end(),
	                           [&](const DistributedNode &n) { return n.host == host && n.port == port; }),
	            nodes.end());
}

vector<DistributedNode> DistributedContext::GetNodes() const {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	return nodes;
}

//===--------------------------------------------------------------------===//
// Worker server lifecycle
//===--------------------------------------------------------------------===//

void DistributedContext::StartWorker(DatabaseInstance &db, int32_t port) {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	if (worker_server) {
		throw std::runtime_error("A worker server is already running on port " + to_string(worker_server->GetPort()));
	}
	auto srv = make_uniq<WorkerServer>(db, port);
	srv->Start();
	worker_server = std::move(srv);
}

void DistributedContext::StopWorker() {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	if (!worker_server) {
		throw std::runtime_error("No worker server is currently running");
	}
	worker_server->Stop();
	worker_server.reset();
}

bool DistributedContext::HasWorker() const {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	return worker_server != nullptr && worker_server->IsRunning();
}

int32_t DistributedContext::GetWorkerPort() const {
	std::lock_guard<std::mutex> lock(nodes_mutex);
	if (!worker_server) {
		return -1;
	}
	return worker_server->GetPort();
}

//===--------------------------------------------------------------------===//
// TCP client helpers
//===--------------------------------------------------------------------===//

namespace {

//! Read a '\n'-terminated line from fd.
static string ReadLineClient(int fd) {
	string line;
	char ch;
	while (true) {
		ssize_t n = recv(fd, &ch, 1, 0);
		if (n <= 0) {
			throw std::runtime_error("Connection closed while reading line from worker");
		}
		if (ch == '\n') {
			break;
		}
		line += ch;
	}
	return line;
}

//! Write all bytes of msg to fd.
static void SendAll(int fd, const string &msg) {
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

//! Write line + '\n' to fd.
static void SendLineClient(int fd, const string &line) {
	SendAll(fd, line + "\n");
}

//! Open a TCP connection to host:port. Returns the connected fd or throws.
static int ConnectToNode(const string &host, int32_t port) {
	struct addrinfo hints {};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res = nullptr;
	int rc = getaddrinfo(host.c_str(), to_string(port).c_str(), &hints, &res);
	if (rc != 0) {
		throw std::runtime_error("getaddrinfo failed for " + host + ":" + to_string(port) + " – " +
		                         gai_strerror(rc));
	}

	int fd = -1;
	for (struct addrinfo *rp = res; rp != nullptr; rp = rp->ai_next) {
		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd < 0) {
			continue;
		}
		if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
			break; // success
		}
		close(fd);
		fd = -1;
	}
	freeaddrinfo(res);

	if (fd < 0) {
		throw std::runtime_error("Cannot connect to worker node " + host + ":" + to_string(port));
	}
	return fd;
}

} // anonymous namespace

//===--------------------------------------------------------------------===//
// Query dispatch
//===--------------------------------------------------------------------===//

NodeQueryResult DistributedContext::QueryNode(const DistributedNode &node, const string &query) {
	NodeQueryResult result;

	int fd = -1;
	try {
		fd = ConnectToNode(node.host, node.port);

		// Send query.
		SendLineClient(fd, "QUERY");
		SendLineClient(fd, to_string(static_cast<int>(query.size())));
		SendAll(fd, query);
		SendLineClient(fd, ""); // trailing newline after SQL bytes

		// Read status line.
		string status = ReadLineClient(fd);
		if (status == "ERR") {
			result.success = false;
			result.error = ReadLineClient(fd);
			close(fd);
			return result;
		}
		if (status != "OK") {
			result.success = false;
			result.error = "Unexpected status from worker: " + status;
			close(fd);
			return result;
		}

		// Read column count.
		int num_cols = std::stoi(ReadLineClient(fd));

		// Read column descriptors.
		for (int col = 0; col < num_cols; col++) {
			string descriptor = ReadLineClient(fd);
			auto tab_pos = descriptor.find('\t');
			if (tab_pos == string::npos) {
				result.success = false;
				result.error = "Malformed column descriptor: " + descriptor;
				close(fd);
				return result;
			}
			result.col_names.push_back(descriptor.substr(0, tab_pos));
			result.col_types.push_back(descriptor.substr(tab_pos + 1));
		}

		// Expect "DATA" marker.
		string data_marker = ReadLineClient(fd);
		if (data_marker != "DATA") {
			result.success = false;
			result.error = "Expected DATA marker, got: " + data_marker;
			close(fd);
			return result;
		}

		// Read rows until "DONE".
		while (true) {
			string row_line = ReadLineClient(fd);
			if (row_line == "DONE") {
				break;
			}

			NodeQueryResult::Row row;
			// Split on tab.
			string cell;
			for (char ch : row_line) {
				if (ch == '\t') {
					if (cell == "\\N") {
						row.values.push_back("");
						row.is_null.push_back(true);
					} else {
						row.values.push_back(UnescapeValue(cell));
						row.is_null.push_back(false);
					}
					cell.clear();
				} else {
					cell += ch;
				}
			}
			// Last cell.
			if (cell == "\\N") {
				row.values.push_back("");
				row.is_null.push_back(true);
			} else {
				row.values.push_back(UnescapeValue(cell));
				row.is_null.push_back(false);
			}

			result.rows.push_back(std::move(row));
		}

		result.success = true;

	} catch (std::exception &e) {
		result.success = false;
		result.error = string(e.what());
	}

	if (fd >= 0) {
		close(fd);
	}
	return result;
}

vector<NodeQueryResult> DistributedContext::QueryAllNodes(const string &query) {
	auto snapshot = GetNodes();
	if (snapshot.empty()) {
		throw InvalidInputException("No distributed nodes registered. Use distributed_add_node() first.");
	}

	vector<NodeQueryResult> results;
	results.reserve(snapshot.size());
	for (auto &node : snapshot) {
		results.push_back(QueryNode(node, query));
	}
	return results;
}

//===--------------------------------------------------------------------===//
// Type / value helpers
//===--------------------------------------------------------------------===//

LogicalType ParseTypeString(const string &type_str) {
	// Normalise to upper-case for comparison.
	string upper = type_str;
	for (char &c : upper) {
		c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
	}

	if (upper == "INTEGER" || upper == "INT" || upper == "INT4" || upper == "SIGNED") {
		return LogicalType::INTEGER;
	}
	if (upper == "BIGINT" || upper == "INT8" || upper == "LONG") {
		return LogicalType::BIGINT;
	}
	if (upper == "SMALLINT" || upper == "INT2" || upper == "SHORT") {
		return LogicalType::SMALLINT;
	}
	if (upper == "TINYINT" || upper == "INT1") {
		return LogicalType::TINYINT;
	}
	if (upper == "HUGEINT") {
		return LogicalType::HUGEINT;
	}
	if (upper == "DOUBLE" || upper == "FLOAT8" || upper == "NUMERIC" || upper == "DECIMAL") {
		return LogicalType::DOUBLE;
	}
	if (upper == "FLOAT" || upper == "FLOAT4" || upper == "REAL") {
		return LogicalType::FLOAT;
	}
	if (upper == "BOOLEAN" || upper == "BOOL" || upper == "LOGICAL") {
		return LogicalType::BOOLEAN;
	}
	if (upper == "VARCHAR" || upper == "TEXT" || upper == "STRING" || upper == "CHAR" || upper == "BPCHAR") {
		return LogicalType::VARCHAR;
	}
	if (upper == "DATE") {
		return LogicalType::DATE;
	}
	if (upper == "TIMESTAMP" || upper == "DATETIME") {
		return LogicalType::TIMESTAMP;
	}
	if (upper == "TIMESTAMP WITH TIME ZONE" || upper == "TIMESTAMPTZ") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (upper == "TIME") {
		return LogicalType::TIME;
	}
	if (upper == "BLOB" || upper == "BYTEA" || upper == "BINARY" || upper == "VARBINARY") {
		return LogicalType::BLOB;
	}
	if (upper == "UBIGINT") {
		return LogicalType::UBIGINT;
	}
	if (upper == "UINTEGER") {
		return LogicalType::UINTEGER;
	}
	if (upper == "USMALLINT") {
		return LogicalType::USMALLINT;
	}
	if (upper == "UTINYINT") {
		return LogicalType::UTINYINT;
	}
	if (upper == "UUID") {
		return LogicalType::UUID;
	}
	if (upper == "JSON") {
		return LogicalType::VARCHAR; // treat as text
	}
	// Unknown type – fall back to VARCHAR so results remain usable.
	return LogicalType::VARCHAR;
}

Value ValueFromString(const string &str, const LogicalType &type) {
	try {
		switch (type.id()) {
		case LogicalTypeId::INTEGER:
			return Value::INTEGER(std::stoi(str));
		case LogicalTypeId::BIGINT:
			return Value::BIGINT(std::stoll(str));
		case LogicalTypeId::SMALLINT:
			return Value::SMALLINT(static_cast<int16_t>(std::stoi(str)));
		case LogicalTypeId::TINYINT:
			return Value::TINYINT(static_cast<int8_t>(std::stoi(str)));
		case LogicalTypeId::HUGEINT: {
			hugeint_t v;
			// HugeInt::FromString is internal; use stoll for values that fit.
			v.lower = static_cast<uint64_t>(std::stoll(str));
			v.upper = (std::stoll(str) < 0) ? -1 : 0;
			return Value::HUGEINT(v);
		}
		case LogicalTypeId::UBIGINT:
			return Value::UBIGINT(std::stoull(str));
		case LogicalTypeId::UINTEGER:
			return Value::UINTEGER(static_cast<uint32_t>(std::stoull(str)));
		case LogicalTypeId::USMALLINT:
			return Value::USMALLINT(static_cast<uint16_t>(std::stoull(str)));
		case LogicalTypeId::UTINYINT:
			return Value::UTINYINT(static_cast<uint8_t>(std::stoull(str)));
		case LogicalTypeId::DOUBLE:
			return Value::DOUBLE(std::stod(str));
		case LogicalTypeId::FLOAT:
			return Value::FLOAT(std::stof(str));
		case LogicalTypeId::BOOLEAN:
			return Value::BOOLEAN(str == "true" || str == "t" || str == "1" || str == "True" || str == "TRUE");
		case LogicalTypeId::DATE:
			return Value::DATE(Date::FromString(str));
		case LogicalTypeId::TIMESTAMP:
			return Value::TIMESTAMP(Timestamp::FromString(str, /*use_offset=*/false));
		case LogicalTypeId::BLOB:
			return Value::BLOB(str);
		case LogicalTypeId::VARCHAR:
		default:
			return Value(str);
		}
	} catch (...) {
		// If parsing fails, return the string representation so results
		// remain usable even if type mapping is imperfect.
		return Value(str);
	}
}

string EscapeValue(const string &str) {
	string out;
	out.reserve(str.size());
	for (char c : str) {
		if (c == '\\') {
			out += "\\\\";
		} else if (c == '\t') {
			out += "\\t";
		} else if (c == '\n') {
			out += "\\n";
		} else if (c == '\r') {
			out += "\\r";
		} else {
			out += c;
		}
	}
	return out;
}

string UnescapeValue(const string &str) {
	string out;
	out.reserve(str.size());
	for (size_t i = 0; i < str.size(); i++) {
		if (str[i] == '\\' && i + 1 < str.size()) {
			char next = str[i + 1];
			if (next == '\\') {
				out += '\\';
				i++;
			} else if (next == 't') {
				out += '\t';
				i++;
			} else if (next == 'n') {
				out += '\n';
				i++;
			} else if (next == 'r') {
				out += '\r';
				i++;
			} else {
				out += str[i];
			}
		} else {
			out += str[i];
		}
	}
	return out;
}

} // namespace duckdb
