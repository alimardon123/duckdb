//===----------------------------------------------------------------------===//
//                         DuckDB
//
// distributed_context.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/function/replacement_scan.hpp"

#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

class WorkerServer;

//! A registered worker node in the distributed cluster.
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

//! Metadata for a distributed (sharded) table.
struct DistributedTableMeta {
	string table_name;
	//! Column used for hash-based INSERT routing. Empty = no routing (broadcast).
	string shard_key;
	//! Number of shards (= number of nodes at creation time).
	int32_t num_shards = 0;
};

//! Result of executing a SELECT query on a single worker node.
struct NodeQueryResult {
	bool success = false;
	string error;
	vector<string> col_names;
	vector<string> col_types;

	struct Row {
		vector<string> values;
		vector<bool> is_null;
	};
	vector<Row> rows;

	//! Node this result came from (populated by QueryAllNodes).
	DistributedNode node;
};

//! Result of executing a DML/DDL statement on a single worker node.
struct NodeExecResult {
	DistributedNode node;
	bool success = false;
	int64_t rows_affected = 0;
	string error;
};

//! Process-wide singleton: cluster topology, distributed table registry,
//! optional local WorkerServer, and TCP client logic.
class DistributedContext {
public:
	static DistributedContext &Get();

	// -----------------------------------------------------------------------
	// Node registry
	// -----------------------------------------------------------------------

	void AddNode(const string &host, int32_t port);
	void RemoveNode(const string &host, int32_t port);
	vector<DistributedNode> GetNodes() const;

	// -----------------------------------------------------------------------
	// Worker server lifecycle
	// -----------------------------------------------------------------------

	//! Start local TCP server. Auto-registers 127.0.0.1:port as a node.
	void StartWorker(DatabaseInstance &db, int32_t port);
	void StopWorker();
	bool HasWorker() const;
	int32_t GetWorkerPort() const;

	// -----------------------------------------------------------------------
	// Distributed table registry
	// -----------------------------------------------------------------------

	//! Register a table as distributed (called from distributed_create_table).
	void RegisterTable(const string &table_name, const string &shard_key, int32_t num_shards);

	//! Unregister a distributed table (called from distributed_drop_table).
	void UnregisterTable(const string &table_name);

	//! Returns true if table_name is a registered distributed table.
	bool IsDistributedTable(const string &table_name) const;

	//! Returns metadata for all registered distributed tables.
	vector<DistributedTableMeta> GetDistributedTables() const;

	//! Given a shard key value (as string), return the node that owns that shard.
	//! Returns empty node {"",-1} if routing metadata is unavailable.
	DistributedNode GetShardNode(const string &table_name, const string &shard_key_value) const;

	// -----------------------------------------------------------------------
	// Replacement scan — registered with DBConfig so that
	//   SELECT * FROM orders
	// transparently fans out to all shards when 'orders' is a distributed table.
	// -----------------------------------------------------------------------
	static unique_ptr<TableRef> ReplacementScan(ClientContext &context, ReplacementScanInput &input,
	                                            optional_ptr<ReplacementScanData> data);

	// -----------------------------------------------------------------------
	// TCP client — SELECT fan-out
	// -----------------------------------------------------------------------

	NodeQueryResult QueryNode(const DistributedNode &node, const string &query);
	//! Parallel fan-out: contacts all nodes simultaneously.
	vector<NodeQueryResult> QueryAllNodes(const string &query);

	// -----------------------------------------------------------------------
	// TCP client — DML/DDL broadcast
	// -----------------------------------------------------------------------

	NodeExecResult ExecNode(const DistributedNode &node, const string &sql);
	//! Parallel broadcast: contacts all nodes simultaneously.
	vector<NodeExecResult> ExecAllNodes(const string &sql);

private:
	mutable std::mutex nodes_mutex;
	vector<DistributedNode> nodes;
	unique_ptr<WorkerServer> worker_server;

	mutable std::mutex tables_mutex;
	vector<DistributedTableMeta> distributed_tables;
};

// ---------------------------------------------------------------------------
// Utility helpers (used by both worker_server.cpp and distributed_context.cpp)
// ---------------------------------------------------------------------------

LogicalType ParseTypeString(const string &type_str);
Value ValueFromString(const string &str, const LogicalType &type);
string EscapeValue(const string &str);
string UnescapeValue(const string &str);

} // namespace duckdb
