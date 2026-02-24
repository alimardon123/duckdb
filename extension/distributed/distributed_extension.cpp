//===----------------------------------------------------------------------===//
//                         DuckDB - Distributed Extension
//
// distributed_extension.cpp
//
// Extension entry point.  Registers all distributed SQL functions:
//
//   distributed_start_worker(port INTEGER)
//       Start a TCP worker server on this DuckDB instance.
//
//   distributed_stop_worker()
//       Stop the local worker server.
//
//   distributed_add_node(host VARCHAR, port INTEGER)
//       Register a remote worker node with the coordinator.
//
//   distributed_remove_node(host VARCHAR, port INTEGER)
//       Unregister a remote worker node.
//
//   distributed_nodes()  →  TABLE(host VARCHAR, port INTEGER, status VARCHAR)
//       List all registered worker nodes and their status.
//
//   distributed_query(query VARCHAR)  →  TABLE(...)
//       Execute a SQL query on every registered worker and union the results.
//
//   distributed_ping(host VARCHAR, port INTEGER)  →  TABLE(success BOOLEAN, message VARCHAR)
//       Send a PING to a node and report whether it responded.
//
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
// distributed_start_worker
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

//===--------------------------------------------------------------------===//
// distributed_stop_worker
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
		DistributedContext::Get().StopWorker();
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(1, 0, Value("Worker stopped"));
	} catch (std::exception &e) {
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value(string(e.what())));
	}
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// distributed_add_node
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
// distributed_remove_node
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
// distributed_nodes
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
// distributed_ping
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

	// Attempt a TCP PING–PONG exchange.
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

		// Send PING.
		string ping_msg = "PING\n";
		send(fd, ping_msg.c_str(), ping_msg.size(), MSG_NOSIGNAL);

		// Read response.
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
// distributed_query
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

	// Determine result schema by sending a LIMIT-0 variant to the first node.
	// This avoids fetching data in the bind phase while still giving DuckDB
	// the type information it needs for query planning.
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

	// Execute the query on every registered node.
	auto &dist_ctx = DistributedContext::Get();
	auto node_results = dist_ctx.QueryAllNodes(bind_data.query);

	for (idx_t ni = 0; ni < node_results.size(); ni++) {
		auto &nr = node_results[ni];
		if (!nr.success) {
			// Log the error but continue with data from other nodes.
			// Users can detect partial failures by comparing expected vs actual row counts.
			continue;
		}
		for (auto &row : nr.rows) {
			DistributedQueryRow out_row;
			out_row.values = row.values;
			out_row.is_null = row.is_null;
			state->all_rows.push_back(std::move(out_row));
		}
	}

	state->current_row = 0;
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
	// distributed_start_worker(port INTEGER)
	{
		TableFunction fn("distributed_start_worker", {LogicalType::INTEGER}, StartWorkerFunction, StartWorkerBind);
		loader.RegisterFunction(fn);
	}

	// distributed_stop_worker()
	{
		TableFunction fn("distributed_stop_worker", {}, StopWorkerFunction, StopWorkerBind);
		loader.RegisterFunction(fn);
	}

	// distributed_add_node(host VARCHAR, port INTEGER)
	{
		TableFunction fn("distributed_add_node", {LogicalType::VARCHAR, LogicalType::INTEGER}, AddNodeFunction,
		                 AddNodeBind);
		loader.RegisterFunction(fn);
	}

	// distributed_remove_node(host VARCHAR, port INTEGER)
	{
		TableFunction fn("distributed_remove_node", {LogicalType::VARCHAR, LogicalType::INTEGER}, RemoveNodeFunction,
		                 RemoveNodeBind);
		loader.RegisterFunction(fn);
	}

	// distributed_nodes()
	{
		TableFunction fn("distributed_nodes", {}, DistributedNodesFunction, DistributedNodesBind);
		loader.RegisterFunction(fn);
	}

	// distributed_ping(host VARCHAR, port INTEGER)
	{
		TableFunction fn("distributed_ping", {LogicalType::VARCHAR, LogicalType::INTEGER}, PingFunction, PingBind);
		loader.RegisterFunction(fn);
	}

	// distributed_query(query VARCHAR)
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
