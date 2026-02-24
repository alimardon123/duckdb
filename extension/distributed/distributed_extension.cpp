//===----------------------------------------------------------------------===//
//                         DuckDB - Distributed Extension
//
// distributed_extension.cpp
//
// Registers all distributed SQL functions.  Every function is a Table
// Function so results can be further processed with standard SQL.
//
// ┌─────────────────────────────────────────────────────────────┐
// │  SETUP (run once per node at startup)                       │
// │                                                             │
// │  distributed_start_worker(port)                             │
// │    – opens TCP server on <port>                             │
// │    – auto-registers 127.0.0.1:<port> as a cluster node so   │
// │      this instance is BOTH coordinator and worker           │
// │                                                             │
// │  distributed_add_node(host, port)                           │
// │    – register a remote worker with this coordinator         │
// │                                                             │
// ├─────────────────────────────────────────────────────────────┤
// │  READ  (fan-out to all nodes, union results)                │
// │                                                             │
// │  distributed_query(sql)                                     │
// │    → TABLE(... same columns as sql ...)                     │
// │                                                             │
// ├─────────────────────────────────────────────────────────────┤
// │  WRITE / DDL  (broadcast to all nodes)                      │
// │                                                             │
// │  distributed_exec(sql)                                      │
// │    → TABLE(node_host, node_port, success,                   │
// │            rows_affected, message)                          │
// │                                                             │
// │  distributed_exec_on(host, port, sql)                       │
// │    → same schema, targets ONE specific node                 │
// │                                                             │
// ├─────────────────────────────────────────────────────────────┤
// │  MANAGEMENT                                                 │
// │                                                             │
// │  distributed_nodes()                                        │
// │  distributed_ping(host, port)                               │
// │  distributed_remove_node(host, port)                        │
// │  distributed_stop_worker()                                  │
// └─────────────────────────────────────────────────────────────┘
//===----------------------------------------------------------------------===//

#include "distributed_extension.hpp"
#include "distributed_context.hpp"
#include "worker_server.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers shared across exec functions
//===--------------------------------------------------------------------===//

//! Populate a fixed-schema exec-result row into a DataChunk at position idx.
//! Schema: node_host VARCHAR, node_port INTEGER, success BOOLEAN,
//!         rows_affected BIGINT, message VARCHAR
static void WriteExecRow(DataChunk &output, idx_t idx, const NodeExecResult &r) {
	output.SetValue(0, idx, Value(r.node.host));
	output.SetValue(1, idx, Value::INTEGER(r.node.port));
	output.SetValue(2, idx, Value::BOOLEAN(r.success));
	output.SetValue(3, idx, Value::BIGINT(r.rows_affected));
	output.SetValue(4, idx, Value(r.success ? "" : r.error));
}

static void AddExecResultColumns(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("node_host");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("node_port");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("rows_affected");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
}

//===--------------------------------------------------------------------===//
// distributed_start_worker(port INTEGER)
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
		output.SetValue(1, 0, Value("Worker started on port " + to_string(data.port) +
		                            " — registered as 127.0.0.1:" + to_string(data.port)));
	} catch (std::exception &e) {
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value(string(e.what())));
	}
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_stop_worker()
//===--------------------------------------------------------------------===//

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
		// Also remove the self-registration added by StartWorker.
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

//===--------------------------------------------------------------------===//
// distributed_add_node(host VARCHAR, port INTEGER)
//===--------------------------------------------------------------------===//

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

	try {
		DistributedContext::Get().AddNode(data.host, data.port);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(1, 0, Value("Node " + data.host + ":" + to_string(data.port) + " registered"));
	} catch (std::exception &e) {
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value(string(e.what())));
	}
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_remove_node(host VARCHAR, port INTEGER)
//===--------------------------------------------------------------------===//

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

//===--------------------------------------------------------------------===//
// distributed_nodes()
//===--------------------------------------------------------------------===//

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

	idx_t count = data.nodes.size();
	for (idx_t i = 0; i < count; i++) {
		output.SetValue(0, i, Value(data.nodes[i].host));
		output.SetValue(1, i, Value::INTEGER(data.nodes[i].port));
		output.SetValue(2, i, Value(data.nodes[i].Address()));
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// distributed_ping(host VARCHAR, port INTEGER)
//===--------------------------------------------------------------------===//

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
		int rc = getaddrinfo(data.host.c_str(), to_string(data.port).c_str(), &hints, &res);
		if (rc != 0) {
			throw std::runtime_error(string(gai_strerror(rc)));
		}

		int fd = -1;
		for (struct addrinfo *rp = res; rp != nullptr; rp = rp->ai_next) {
			fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (fd < 0) {
				continue;
			}
			if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
				break;
			}
			close(fd);
			fd = -1;
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

		if (resp == "PONG") {
			ok = true;
			msg = "Node " + data.host + ":" + to_string(data.port) + " is alive";
		} else {
			msg = "Unexpected response: " + resp;
		}
	} catch (std::exception &e) {
		msg = string(e.what());
	}

	output.SetValue(0, 0, Value::BOOLEAN(ok));
	output.SetValue(1, 0, Value(msg));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_exec(sql VARCHAR)
//   Broadcast any SQL (INSERT/UPDATE/DELETE/CREATE/DROP/…) to ALL nodes.
//   Returns one status row per node.
//===--------------------------------------------------------------------===//

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
		throw InvalidInputException("No distributed nodes registered. "
		                            "Use SELECT * FROM distributed_add_node('host', port) first.");
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
		WriteExecRow(output, count, state.results[state.current]);
		state.current++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// distributed_exec_on(host VARCHAR, port INTEGER, sql VARCHAR)
//   Run SQL on ONE specific node.  Useful for targeted inserts when you
//   know exactly which shard owns the data.
//===--------------------------------------------------------------------===//

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
	auto &bind_data = input.bind_data->Cast<DistributedExecOnBindData>();
	auto state = make_uniq<DistributedExecOnGlobalState>();
	DistributedNode node {bind_data.host, bind_data.port};
	state->result = DistributedContext::Get().ExecNode(node, bind_data.sql);
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

//===--------------------------------------------------------------------===//
// distributed_query(sql VARCHAR)
//   Fan-out a SELECT to ALL nodes and union results (existing, now parallel).
//===--------------------------------------------------------------------===//

struct DistributedQueryBindData : public TableFunctionData {
	string query;
	vector<LogicalType> col_types;
	vector<string> col_names;
	vector<DistributedNode> nodes;
};

struct DistributedQueryRow {
	vector<string> values;
	vector<bool> is_null;
};

struct DistributedQueryGlobalState : public GlobalTableFunctionState {
	vector<DistributedQueryRow> all_rows;
	idx_t current_row = 0;
	//! Nodes that failed — available for inspection but we continue with others.
	vector<string> failed_nodes;
};

static unique_ptr<FunctionData> DistributedQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DistributedQueryBindData>();
	result->query = StringValue::Get(input.inputs[0]);

	auto &dist_ctx = DistributedContext::Get();
	result->nodes = dist_ctx.GetNodes();

	if (result->nodes.empty()) {
		throw InvalidInputException("No distributed nodes registered. "
		                            "Use SELECT * FROM distributed_add_node('host', port) first.");
	}

	// Probe schema from node[0] using a LIMIT 0 wrapper — zero data transfer.
	string schema_query = "SELECT * FROM (" + result->query + ") __dist_schema__ LIMIT 0";
	NodeQueryResult schema = dist_ctx.QueryNode(result->nodes[0], schema_query);

	if (!schema.success) {
		throw IOException("Failed to get schema from node " + result->nodes[0].Address() + ": " + schema.error);
	}
	if (schema.col_names.empty()) {
		throw IOException("Query returned no columns on node " + result->nodes[0].Address());
	}

	result->col_names = schema.col_names;
	for (auto &type_str : schema.col_types) {
		result->col_types.push_back(ParseTypeString(type_str));
	}

	names = result->col_names;
	return_types = result->col_types;

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DistributedQueryInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DistributedQueryBindData>();
	auto state = make_uniq<DistributedQueryGlobalState>();

	// QueryAllNodes is now parallel — all nodes are contacted simultaneously.
	auto &dist_ctx = DistributedContext::Get();
	auto node_results = dist_ctx.QueryAllNodes(bind_data.query);

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

	// Surface partial failures as a warning in the message field.
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
	auto &bind_data = data_p.bind_data->Cast<DistributedQueryBindData>();
	auto &state = data_p.global_state->Cast<DistributedQueryGlobalState>();

	if (state.current_row >= state.all_rows.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t num_cols = bind_data.col_types.size();

	while (state.current_row < state.all_rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.all_rows[state.current_row];
		for (idx_t col = 0; col < num_cols && col < row.values.size(); col++) {
			if (row.is_null[col]) {
				output.SetValue(col, count, Value(bind_data.col_types[col]));
			} else {
				output.SetValue(col, count, ValueFromString(row.values[col], bind_data.col_types[col]));
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
	// ── Setup ──────────────────────────────────────────────────────────────
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

	// ── Read ───────────────────────────────────────────────────────────────
	{
		TableFunction fn("distributed_query", {LogicalType::VARCHAR}, DistributedQueryScan, DistributedQueryBind,
		                 DistributedQueryInit);
		loader.RegisterFunction(fn);
	}

	// ── Write / DDL ────────────────────────────────────────────────────────
	{
		// Broadcast to all nodes.
		TableFunction fn("distributed_exec", {LogicalType::VARCHAR}, DistributedExecScan, DistributedExecBind,
		                 DistributedExecInit);
		loader.RegisterFunction(fn);
	}
	{
		// Target one specific node.
		TableFunction fn("distributed_exec_on",
		                 {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, DistributedExecOnScan,
		                 DistributedExecOnBind, DistributedExecOnInit);
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
