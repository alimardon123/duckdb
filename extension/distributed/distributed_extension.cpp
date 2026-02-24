//===----------------------------------------------------------------------===//
//                         DuckDB - Distributed Extension
//
// distributed_extension.cpp
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │  CLUSTER SETUP  (once per process)                                       │
// │                                                                          │
// │  distributed_setup(port)          – start worker, self-register,         │
// │                                     enable replacement scan              │
// │  distributed_setup(port, peers)   – same + parse "host:port,…" list      │
// │  distributed_add_node(host, port) – register additional remote node      │
// │  distributed_remove_node(h, port) – unregister a node                    │
// │  distributed_nodes()              – list cluster nodes                   │
// │  distributed_ping(host, port)     – health check                         │
// │  distributed_stop_worker()        – stop local TCP server                │
// │                                                                          │
// ├──────────────────────────────────────────────────────────────────────────┤
// │  TABLE MANAGEMENT  (DDL)                                                 │
// │                                                                          │
// │  distributed_create_table(name, schema, shard_key)                       │
// │    – CREATE TABLE on every node + register for transparent access        │
// │  distributed_drop_table(name)     – DROP TABLE everywhere + unregister   │
// │  distributed_tables()             – list registered distributed tables   │
// │                                                                          │
// ├──────────────────────────────────────────────────────────────────────────┤
// │  TRANSPARENT READ  (no function call needed after setup)                 │
// │                                                                          │
// │  SELECT * FROM orders             – replacement scan intercepts this,    │
// │                                     fans out to all shards automatically │
// │  SELECT SUM(x) FROM orders        – aggregation on coordinator           │
// │  SELECT * FROM orders WHERE k=v   – WHERE applied locally after fan-out  │
// │                                                                          │
// ├──────────────────────────────────────────────────────────────────────────┤
// │  WRITES  (broadcast or shard-routed)                                     │
// │                                                                          │
// │  distributed_exec(sql)            – broadcast to ALL nodes               │
// │  distributed_exec_on(h, port, sql)– target ONE specific node             │
// │  distributed_exec_shard(tbl,key,sql) – auto-route to correct shard       │
// │                                                                          │
// │  distributed_query(sql)           – explicit fan-out SELECT (advanced)   │
// └──────────────────────────────────────────────────────────────────────────┘
//===----------------------------------------------------------------------===//

#include "distributed_extension.hpp"
#include "distributed_context.hpp"
#include "worker_server.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Shared helpers
//===--------------------------------------------------------------------===//

static void AddExecResultColumns(vector<LogicalType> &rt, vector<string> &names) {
	rt.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("node_host");
	rt.emplace_back(LogicalType::INTEGER);
	names.emplace_back("node_port");
	rt.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	rt.emplace_back(LogicalType::BIGINT);
	names.emplace_back("rows_affected");
	rt.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
}

static void WriteExecRow(DataChunk &output, idx_t idx, const NodeExecResult &r) {
	output.SetValue(0, idx, Value(r.node.host));
	output.SetValue(1, idx, Value::INTEGER(r.node.port));
	output.SetValue(2, idx, Value::BOOLEAN(r.success));
	output.SetValue(3, idx, Value::BIGINT(r.rows_affected));
	output.SetValue(4, idx, Value(r.success ? "" : r.error));
}

//===--------------------------------------------------------------------===//
// distributed_setup(port INTEGER [, peers VARCHAR])
//
//  port  – local TCP port this instance will listen on.
//  peers – optional comma-separated "host:port" list of remote nodes.
//          e.g.  '10.0.0.2:9876,10.0.0.3:9876'
//
// After this call:
//   • Local worker is running.
//   • This node is automatically registered (coordinator = worker).
//   • Any peers are parsed and registered.
//   • Replacement scan is active (transparent SELECT from distributed tables).
//===--------------------------------------------------------------------===//

struct SetupBindData : public TableFunctionData {
	int32_t port = 0;
	string peers; // comma-separated host:port pairs
	bool finished = false;
};

static unique_ptr<FunctionData> SetupBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SetupBindData>();
	result->port = IntegerValue::Get(input.inputs[0]);
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		result->peers = StringValue::Get(input.inputs[1]);
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return std::move(result);
}

static void SetupFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<SetupBindData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;

	auto &db = DatabaseInstance::GetDatabase(context);
	string msg;
	bool ok = true;

	try {
		DistributedContext::Get().StartWorker(db, data.port);
		msg = "Worker started on port " + to_string(data.port);

		// Parse peer list: "host1:port1,host2:port2"
		if (!data.peers.empty()) {
			auto peer_list = StringUtil::Split(data.peers, ',');
			int registered = 0;
			for (auto &peer : peer_list) {
				auto trimmed = peer;
				StringUtil::Trim(trimmed);
				if (trimmed.empty()) {
					continue;
				}
				auto colon = trimmed.rfind(':');
				if (colon == string::npos) {
					continue;
				}
				string host = trimmed.substr(0, colon);
				int32_t port = std::stoi(trimmed.substr(colon + 1));
				DistributedContext::Get().AddNode(host, port);
				registered++;
			}
			msg += " + " + to_string(registered) + " peer(s) registered";
		}
	} catch (std::exception &e) {
		ok = false;
		msg = string(e.what());
	}

	output.SetValue(0, 0, Value::BOOLEAN(ok));
	output.SetValue(1, 0, Value(msg));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_create_table(name, schema, shard_key)
//
//  name      – table name (must be valid SQL identifier)
//  schema    – column definitions, e.g. 'id BIGINT, name VARCHAR, val DOUBLE'
//  shard_key – column name used for hash-based INSERT routing,
//              e.g. 'id'.  Pass '' to disable routing (broadcast writes).
//
// Creates the table on every registered node and registers it so that
// plain  SELECT * FROM <name>  fans out transparently.
//===--------------------------------------------------------------------===//

struct CreateTableBindData : public TableFunctionData {
	string table_name;
	string schema;
	string shard_key;
	bool finished = false;
};

static unique_ptr<FunctionData> CreateTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CreateTableBindData>();
	result->table_name = StringValue::Get(input.inputs[0]);
	result->schema = StringValue::Get(input.inputs[1]);
	result->shard_key = (input.inputs.size() > 2 && !input.inputs[2].IsNull()) ? StringValue::Get(input.inputs[2])
	                                                                             : "";

	if (DistributedContext::Get().GetNodes().empty()) {
		throw InvalidInputException("No nodes registered. Run distributed_setup() first.");
	}

	AddExecResultColumns(return_types, names);
	return std::move(result);
}

struct CreateTableGlobalState : public GlobalTableFunctionState {
	vector<NodeExecResult> results;
	idx_t current = 0;
};

static unique_ptr<GlobalTableFunctionState> CreateTableInit(ClientContext &context,
                                                             TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CreateTableBindData>();
	auto state = make_uniq<CreateTableGlobalState>();

	string create_sql = "CREATE TABLE IF NOT EXISTS \"" + bind_data.table_name + "\" (" + bind_data.schema + ")";
	state->results = DistributedContext::Get().ExecAllNodes(create_sql);

	// Register in distributed table registry so the replacement scan fires.
	bool any_ok = false;
	for (auto &r : state->results) {
		if (r.success) {
			any_ok = true;
		}
	}
	if (any_ok) {
		auto nodes = DistributedContext::Get().GetNodes();
		DistributedContext::Get().RegisterTable(bind_data.table_name, bind_data.shard_key,
		                                        static_cast<int32_t>(nodes.size()));
	}

	return std::move(state);
}

static void CreateTableScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<CreateTableGlobalState>();
	if (state.current >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}
	idx_t count = 0;
	while (state.current < state.results.size() && count < STANDARD_VECTOR_SIZE) {
		WriteExecRow(output, count, state.results[state.current++]);
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// distributed_drop_table(name)
//===--------------------------------------------------------------------===//

struct DropTableBindData : public TableFunctionData {
	string table_name;
	bool finished = false;
};

static unique_ptr<FunctionData> DropTableBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DropTableBindData>();
	result->table_name = StringValue::Get(input.inputs[0]);
	AddExecResultColumns(return_types, names);
	return std::move(result);
}

struct DropTableGlobalState : public GlobalTableFunctionState {
	vector<NodeExecResult> results;
	idx_t current = 0;
};

static unique_ptr<GlobalTableFunctionState> DropTableInit(ClientContext &context,
                                                           TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DropTableBindData>();
	auto state = make_uniq<DropTableGlobalState>();

	string drop_sql = "DROP TABLE IF EXISTS \"" + bind_data.table_name + "\"";
	state->results = DistributedContext::Get().ExecAllNodes(drop_sql);
	DistributedContext::Get().UnregisterTable(bind_data.table_name);

	return std::move(state);
}

static void DropTableScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DropTableGlobalState>();
	if (state.current >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}
	idx_t count = 0;
	while (state.current < state.results.size() && count < STANDARD_VECTOR_SIZE) {
		WriteExecRow(output, count, state.results[state.current++]);
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// distributed_tables()
//===--------------------------------------------------------------------===//

struct DistributedTablesData : public TableFunctionData {
	vector<DistributedTableMeta> tables;
	bool finished = false;
};

static unique_ptr<FunctionData> DistributedTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedTablesData>();
	result->tables = DistributedContext::Get().GetDistributedTables();
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("shard_key");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("num_shards");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("node_count");
	return std::move(result);
}

static void DistributedTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DistributedTablesData>();
	if (data.finished || data.tables.empty()) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	idx_t node_count = static_cast<idx_t>(DistributedContext::Get().GetNodes().size());
	for (idx_t i = 0; i < data.tables.size(); i++) {
		output.SetValue(0, i, Value(data.tables[i].table_name));
		output.SetValue(1, i, Value(data.tables[i].shard_key));
		output.SetValue(2, i, Value::INTEGER(data.tables[i].num_shards));
		output.SetValue(3, i, Value::INTEGER(static_cast<int32_t>(node_count)));
	}
	output.SetCardinality(data.tables.size());
}

//===--------------------------------------------------------------------===//
// distributed_exec_shard(table, shard_key_value, sql)
//
//  Routes sql to the single node that owns the shard for shard_key_value.
//  Used for targeted INSERTs when the shard key is known.
//
//  Example:
//    SELECT * FROM distributed_exec_shard('orders', '42',
//        'INSERT INTO orders VALUES (42, ''Alice'', 99.99)');
//===--------------------------------------------------------------------===//

struct ExecShardBindData : public TableFunctionData {
	string table_name;
	string shard_key_value;
	string sql;
};

struct ExecShardGlobalState : public GlobalTableFunctionState {
	NodeExecResult result;
	bool returned = false;
};

static unique_ptr<FunctionData> ExecShardBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ExecShardBindData>();
	result->table_name = StringValue::Get(input.inputs[0]);
	result->shard_key_value = StringValue::Get(input.inputs[1]);
	result->sql = StringValue::Get(input.inputs[2]);
	AddExecResultColumns(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ExecShardInit(ClientContext &context,
                                                           TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ExecShardBindData>();
	auto state = make_uniq<ExecShardGlobalState>();

	DistributedNode node =
	    DistributedContext::Get().GetShardNode(bind_data.table_name, bind_data.shard_key_value);
	if (node.port < 0) {
		// No routing info — fall back to first node.
		auto nodes = DistributedContext::Get().GetNodes();
		if (nodes.empty()) {
			throw InvalidInputException("No distributed nodes registered.");
		}
		node = nodes[0];
	}

	state->result = DistributedContext::Get().ExecNode(node, bind_data.sql);
	return std::move(state);
}

static void ExecShardScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ExecShardGlobalState>();
	if (state.returned) {
		output.SetCardinality(0);
		return;
	}
	state.returned = true;
	WriteExecRow(output, 0, state.result);
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_shard_for(table, key_value) → TABLE(node_host, node_port)
//   Utility: show which node a given key value would be routed to.
//===--------------------------------------------------------------------===//

struct ShardForData : public TableFunctionData {
	string table_name;
	string key_value;
	bool finished = false;
};

static unique_ptr<FunctionData> ShardForBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ShardForData>();
	result->table_name = StringValue::Get(input.inputs[0]);
	result->key_value = StringValue::Get(input.inputs[1]);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("node_host");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("node_port");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("address");
	return std::move(result);
}

static void ShardForFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ShardForData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;

	DistributedNode node = DistributedContext::Get().GetShardNode(data.table_name, data.key_value);
	if (node.port < 0) {
		auto nodes = DistributedContext::Get().GetNodes();
		if (!nodes.empty()) {
			node = nodes[0];
		}
	}
	output.SetValue(0, 0, Value(node.host));
	output.SetValue(1, 0, Value::INTEGER(node.port));
	output.SetValue(2, 0, Value(node.Address()));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// Remaining functions (unchanged from previous version)
//===--------------------------------------------------------------------===//

struct StartWorkerData : public TableFunctionData {
	int32_t port = 0;
	bool finished = false;
};
static unique_ptr<FunctionData> StartWorkerBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<StartWorkerData>();
	result->port = IntegerValue::Get(input.inputs[0]);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return std::move(result);
}
static void StartWorkerFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<StartWorkerData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	auto &db = DatabaseInstance::GetDatabase(context);
	try {
		DistributedContext::Get().StartWorker(db, data.port);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(1, 0, Value("Worker started on port " + to_string(data.port)));
	} catch (std::exception &e) {
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value(string(e.what())));
	}
	output.SetCardinality(1);
}

struct StopWorkerData : public TableFunctionData {
	bool finished = false;
};
static unique_ptr<FunctionData> StopWorkerBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return make_uniq<StopWorkerData>();
}
static void StopWorkerFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<StopWorkerData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	try {
		int32_t port = DistributedContext::Get().GetWorkerPort();
		DistributedContext::Get().StopWorker();
		if (port > 0) {
			DistributedContext::Get().RemoveNode("127.0.0.1", port);
		}
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(1, 0, Value("Worker stopped"));
	} catch (std::exception &e) {
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value(string(e.what())));
	}
	output.SetCardinality(1);
}

struct AddNodeData : public TableFunctionData {
	string host;
	int32_t port = 0;
	bool finished = false;
};
static unique_ptr<FunctionData> AddNodeBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AddNodeData>();
	result->host = StringValue::Get(input.inputs[0]);
	result->port = IntegerValue::Get(input.inputs[1]);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return std::move(result);
}
static void AddNodeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<AddNodeData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	DistributedContext::Get().AddNode(data.host, data.port);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value("Node " + data.host + ":" + to_string(data.port) + " registered"));
	output.SetCardinality(1);
}

struct RemoveNodeData : public TableFunctionData {
	string host;
	int32_t port = 0;
	bool finished = false;
};
static unique_ptr<FunctionData> RemoveNodeBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RemoveNodeData>();
	result->host = StringValue::Get(input.inputs[0]);
	result->port = IntegerValue::Get(input.inputs[1]);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return std::move(result);
}
static void RemoveNodeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<RemoveNodeData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	DistributedContext::Get().RemoveNode(data.host, data.port);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value("Node " + data.host + ":" + to_string(data.port) + " removed"));
	output.SetCardinality(1);
}

struct DistributedNodesData : public TableFunctionData {
	vector<DistributedNode> nodes;
	bool finished = false;
};
static unique_ptr<FunctionData> DistributedNodesBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedNodesData>();
	result->nodes = DistributedContext::Get().GetNodes();
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("host");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("port");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("address");
	return std::move(result);
}
static void DistributedNodesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DistributedNodesData>();
	if (data.finished || data.nodes.empty()) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	for (idx_t i = 0; i < data.nodes.size(); i++) {
		output.SetValue(0, i, Value(data.nodes[i].host));
		output.SetValue(1, i, Value::INTEGER(data.nodes[i].port));
		output.SetValue(2, i, Value(data.nodes[i].Address()));
	}
	output.SetCardinality(data.nodes.size());
}

struct PingData : public TableFunctionData {
	string host;
	int32_t port = 0;
	bool finished = false;
};
static unique_ptr<FunctionData> PingBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PingData>();
	result->host = StringValue::Get(input.inputs[0]);
	result->port = IntegerValue::Get(input.inputs[1]);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return std::move(result);
}
static void PingFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<PingData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	data.finished = true;
	bool ok = false;
	string msg;
	try {
		struct addrinfo hints {};
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		struct addrinfo *res = nullptr;
		if (getaddrinfo(data.host.c_str(), to_string(data.port).c_str(), &hints, &res) != 0) {
			throw std::runtime_error("getaddrinfo failed");
		}
		int fd = -1;
		for (auto *rp = res; rp; rp = rp->ai_next) {
			fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (fd >= 0 && connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
				break;
			}
			if (fd >= 0) {
				close(fd);
				fd = -1;
			}
		}
		freeaddrinfo(res);
		if (fd < 0) {
			throw std::runtime_error("Could not connect");
		}
		string ping_msg = "PING\n";
		send(fd, ping_msg.c_str(), ping_msg.size(), MSG_NOSIGNAL);
		string resp;
		char ch;
		while (recv(fd, &ch, 1, 0) > 0 && ch != '\n') {
			resp += ch;
		}
		close(fd);
		ok = (resp == "PONG");
		msg = ok ? "Node " + data.host + ":" + to_string(data.port) + " is alive" : "Unexpected: " + resp;
	} catch (std::exception &e) {
		msg = string(e.what());
	}
	output.SetValue(0, 0, Value::BOOLEAN(ok));
	output.SetValue(1, 0, Value(msg));
	output.SetCardinality(1);
}

// distributed_exec — broadcast to all nodes
struct DistributedExecBindData : public TableFunctionData {
	string sql;
};
struct DistributedExecGlobalState : public GlobalTableFunctionState {
	vector<NodeExecResult> results;
	idx_t current = 0;
};
static unique_ptr<FunctionData> DistributedExecBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedExecBindData>();
	result->sql = StringValue::Get(input.inputs[0]);
	if (DistributedContext::Get().GetNodes().empty()) {
		throw InvalidInputException("No distributed nodes registered.");
	}
	AddExecResultColumns(return_types, names);
	return std::move(result);
}
static unique_ptr<GlobalTableFunctionState> DistributedExecInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DistributedExecBindData>();
	auto state = make_uniq<DistributedExecGlobalState>();
	state->results = DistributedContext::Get().ExecAllNodes(bind_data.sql);
	return std::move(state);
}
static void DistributedExecScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DistributedExecGlobalState>();
	if (state.current >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}
	idx_t count = 0;
	while (state.current < state.results.size() && count < STANDARD_VECTOR_SIZE) {
		WriteExecRow(output, count, state.results[state.current++]);
		count++;
	}
	output.SetCardinality(count);
}

// distributed_exec_on — single specific node
struct DistributedExecOnBindData : public TableFunctionData {
	string host;
	int32_t port = 0;
	string sql;
};
struct DistributedExecOnGlobalState : public GlobalTableFunctionState {
	NodeExecResult result;
	bool returned = false;
};
static unique_ptr<FunctionData> DistributedExecOnBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedExecOnBindData>();
	result->host = StringValue::Get(input.inputs[0]);
	result->port = IntegerValue::Get(input.inputs[1]);
	result->sql = StringValue::Get(input.inputs[2]);
	AddExecResultColumns(return_types, names);
	return std::move(result);
}
static unique_ptr<GlobalTableFunctionState> DistributedExecOnInit(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<DistributedExecOnBindData>();
	auto state = make_uniq<DistributedExecOnGlobalState>();
	DistributedNode node {bd.host, bd.port};
	state->result = DistributedContext::Get().ExecNode(node, bd.sql);
	return std::move(state);
}
static void DistributedExecOnScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DistributedExecOnGlobalState>();
	if (state.returned) {
		output.SetCardinality(0);
		return;
	}
	state.returned = true;
	WriteExecRow(output, 0, state.result);
	output.SetCardinality(1);
}

// distributed_query — explicit fan-out SELECT
struct DistributedQueryBindData : public TableFunctionData {
	string query;
	vector<LogicalType> col_types;
	vector<string> col_names;
};
struct DistributedQueryRow {
	vector<string> values;
	vector<bool> is_null;
};
struct DistributedQueryGlobalState : public GlobalTableFunctionState {
	vector<DistributedQueryRow> all_rows;
	idx_t current_row = 0;
	vector<string> failed_nodes;
};
static unique_ptr<FunctionData> DistributedQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedQueryBindData>();
	result->query = StringValue::Get(input.inputs[0]);

	auto &dist_ctx = DistributedContext::Get();
	auto nodes = dist_ctx.GetNodes();
	if (nodes.empty()) {
		throw InvalidInputException("No distributed nodes registered.");
	}

	// Probe schema from first node.
	string schema_q = "SELECT * FROM (" + result->query + ") __dist_schema__ LIMIT 0";
	NodeQueryResult schema = dist_ctx.QueryNode(nodes[0], schema_q);
	if (!schema.success) {
		throw IOException("Schema probe failed on " + nodes[0].Address() + ": " + schema.error);
	}
	if (schema.col_names.empty()) {
		throw IOException("Query returned no columns on " + nodes[0].Address());
	}

	result->col_names = schema.col_names;
	for (auto &t : schema.col_types) {
		result->col_types.push_back(ParseTypeString(t));
	}
	names = result->col_names;
	return_types = result->col_types;
	return std::move(result);
}
static unique_ptr<GlobalTableFunctionState> DistributedQueryInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<DistributedQueryBindData>();
	auto state = make_uniq<DistributedQueryGlobalState>();
	auto node_results = DistributedContext::Get().QueryAllNodes(bd.query);
	for (auto &nr : node_results) {
		if (!nr.success) {
			state->failed_nodes.push_back(nr.node.Address() + ": " + nr.error);
			continue;
		}
		for (auto &row : nr.rows) {
			DistributedQueryRow out_row;
			out_row.values = row.values;
			out_row.is_null = row.is_null;
			state->all_rows.push_back(std::move(out_row));
		}
	}
	if (!state->failed_nodes.empty() && state->all_rows.empty()) {
		string msg = "All nodes failed:";
		for (auto &e : state->failed_nodes) {
			msg += "\n  " + e;
		}
		throw IOException(msg);
	}
	return std::move(state);
}
static void DistributedQueryScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bd = data_p.bind_data->Cast<DistributedQueryBindData>();
	auto &state = data_p.global_state->Cast<DistributedQueryGlobalState>();
	if (state.current_row >= state.all_rows.size()) {
		output.SetCardinality(0);
		return;
	}
	idx_t count = 0;
	idx_t num_cols = bd.col_types.size();
	while (state.current_row < state.all_rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.all_rows[state.current_row];
		for (idx_t col = 0; col < num_cols && col < row.values.size(); col++) {
			if (row.is_null[col]) {
				output.SetValue(col, count, Value(bd.col_types[col]));
			} else {
				output.SetValue(col, count, ValueFromString(row.values[col], bd.col_types[col]));
			}
		}
		state.current_row++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Extension registration
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();

	// ── Replacement scan (transparent SELECT) ─────────────────────────────
	DBConfig::GetConfig(db).replacement_scans.emplace_back(DistributedContext::ReplacementScan);

	// ── Cluster setup ──────────────────────────────────────────────────────
	{
		// distributed_setup(port)  or  distributed_setup(port, peers_csv)
		TableFunction fn("distributed_setup", {LogicalType::INTEGER}, SetupFunction, SetupBind);
		fn.varargs = LogicalType::VARCHAR;
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_start_worker", {LogicalType::INTEGER}, StartWorkerFunction, StartWorkerBind);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_stop_worker", {}, StopWorkerFunction, StopWorkerBind);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_add_node", {LogicalType::VARCHAR, LogicalType::INTEGER}, AddNodeFunction,
		                 AddNodeBind);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_remove_node", {LogicalType::VARCHAR, LogicalType::INTEGER}, RemoveNodeFunction,
		                 RemoveNodeBind);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_nodes", {}, DistributedNodesFunction, DistributedNodesBind);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_ping", {LogicalType::VARCHAR, LogicalType::INTEGER}, PingFunction, PingBind);
		loader.RegisterFunction(fn);
	}

	// ── Table management ───────────────────────────────────────────────────
	{
		// distributed_create_table(name, schema [, shard_key])
		TableFunction fn("distributed_create_table", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		                 CreateTableScan, CreateTableBind, CreateTableInit);
		fn.varargs = LogicalType::VARCHAR; // optional shard_key
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_drop_table", {LogicalType::VARCHAR}, DropTableScan, DropTableBind,
		                 DropTableInit);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_tables", {}, DistributedTablesFunction, DistributedTablesBind);
		loader.RegisterFunction(fn);
	}

	// ── Writes ─────────────────────────────────────────────────────────────
	{
		TableFunction fn("distributed_exec", {LogicalType::VARCHAR}, DistributedExecScan, DistributedExecBind,
		                 DistributedExecInit);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("distributed_exec_on",
		                 {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, DistributedExecOnScan,
		                 DistributedExecOnBind, DistributedExecOnInit);
		loader.RegisterFunction(fn);
	}
	{
		// distributed_exec_shard(table, key_value, sql)
		TableFunction fn("distributed_exec_shard",
		                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ExecShardScan,
		                 ExecShardBind, ExecShardInit);
		loader.RegisterFunction(fn);
	}
	{
		// Utility: show which node owns a given key
		TableFunction fn("distributed_shard_for", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ShardForFunction,
		                 ShardForBind);
		loader.RegisterFunction(fn);
	}

	// ── Explicit fan-out query ─────────────────────────────────────────────
	{
		TableFunction fn("distributed_query", {LogicalType::VARCHAR}, DistributedQueryScan, DistributedQueryBind,
		                 DistributedQueryInit);
		loader.RegisterFunction(fn);
	}
}

void DistributedExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DistributedExtension::Name() {
	return "distributed";
}

std::string DistributedExtension::Version() const {
#ifdef EXT_VERSION_DISTRIBUTED
	return EXT_VERSION_DISTRIBUTED;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(distributed, loader) {
	duckdb::LoadInternal(loader);
}
}
