//===----------------------------------------------------------------------===//
//                         DuckDB
//
// distributed_context.hpp
//
// Manages the distributed cluster state: registered worker nodes,
// the optional local worker server, and TCP client logic for
// sending queries to remote DuckDB worker nodes.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

class WorkerServer;

//! Represents a single registered worker node in the distributed cluster.
struct DistributedNode {
	string host;
	int32_t port;

	string Address() const {
		return host + ":" + to_string(port);
	}

	bool operator==(const DistributedNode &other) const {
		return host == other.host && port == other.port;
	}
};

//! Result of executing a SELECT (or schema-probe) query on a single worker node.
struct NodeQueryResult {
	bool success = false;
	string error;

	//! Column names returned by the worker.
	vector<string> col_names;
	//! Column type names (e.g. "INTEGER", "VARCHAR") returned by the worker.
	vector<string> col_types;

	struct Row {
		vector<string> values; //! String-encoded cell values.
		vector<bool> is_null;  //! True for NULL cells.
	};
	vector<Row> rows;

	//! The node this result came from (filled in by QueryAllNodes / ExecAllNodes).
	DistributedNode node;
};

//! Result of executing a DML/DDL statement on a single worker node.
//! DuckDB returns a "Count" column for INSERT/UPDATE/DELETE.
struct NodeExecResult {
	//! The node this result came from.
	DistributedNode node;
	bool success = false;
	//! Number of rows affected (populated for INSERT/UPDATE/DELETE).
	int64_t rows_affected = 0;
	//! Error message when success == false.
	string error;
};

//! Global singleton that tracks the cluster state for this DuckDB process.
//! Stores registered worker nodes and manages the optional local WorkerServer.
class DistributedContext {
public:
	//! Returns the process-wide DistributedContext singleton.
	static DistributedContext &Get();

	//! Register a remote worker node. Duplicate (host, port) pairs are ignored.
	void AddNode(const string &host, int32_t port);

	//! Unregister a worker node. No-op if the node is not registered.
	void RemoveNode(const string &host, int32_t port);

	//! Return a snapshot of all registered nodes.
	vector<DistributedNode> GetNodes() const;

	//! Start a local TCP worker server on the given port.
	//! Automatically registers 127.0.0.1:port as a node so this instance
	//! participates in distributed queries as both coordinator and worker.
	//! Fails if a worker is already running.
	void StartWorker(DatabaseInstance &db, int32_t port);

	//! Stop the local worker server if running.
	void StopWorker();

	//! Returns true if a local worker server is currently running.
	bool HasWorker() const;

	//! Returns the port of the running worker server, or -1.
	int32_t GetWorkerPort() const;

	// -----------------------------------------------------------------------
	// SELECT / schema queries  (fan-out, union results)
	// -----------------------------------------------------------------------

	//! Execute a query on a specific node and return the result.
	NodeQueryResult QueryNode(const DistributedNode &node, const string &query);

	//! Execute a query on every registered node IN PARALLEL and return one
	//! result per node (in registration order).
	vector<NodeQueryResult> QueryAllNodes(const string &query);

	// -----------------------------------------------------------------------
	// DML / DDL execution  (broadcast writes)
	// -----------------------------------------------------------------------

	//! Execute a DML/DDL statement on a specific node and return the exec result.
	NodeExecResult ExecNode(const DistributedNode &node, const string &sql);

	//! Execute a DML/DDL statement on every registered node IN PARALLEL and
	//! return one NodeExecResult per node.
	vector<NodeExecResult> ExecAllNodes(const string &sql);

private:
	mutable std::mutex nodes_mutex;
	vector<DistributedNode> nodes;
	unique_ptr<WorkerServer> worker_server;
};

//! Convert a DuckDB type-name string (as returned by LogicalType::ToString())
//! into a LogicalType. Unmapped type names fall back to LogicalType::VARCHAR.
LogicalType ParseTypeString(const string &type_str);

//! Convert a string-encoded cell value into a typed DuckDB Value.
Value ValueFromString(const string &str, const LogicalType &type);

//! Escape special characters (backslash, tab, newline) for TSV wire format.
string EscapeValue(const string &str);

//! Inverse of EscapeValue.
string UnescapeValue(const string &str);

} // namespace duckdb
