//===----------------------------------------------------------------------===//
// mwdb_functions.cpp  –  Table/scalar function implementations
//===----------------------------------------------------------------------===//
#include "functions/mwdb_functions.hpp"
#include "mwdb_format.hpp"
#include "mwdb_pk_index.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <chrono>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <set>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Utility: current unix timestamp in milliseconds
//===--------------------------------------------------------------------===//

static uint64_t NowMs() {
    auto now = std::chrono::system_clock::now();
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

//===--------------------------------------------------------------------===//
// Utility: get file size
//===--------------------------------------------------------------------===//

static uint64_t GetFileSize(const std::string &path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return 0;
    return (uint64_t)f.tellg();
}

//===--------------------------------------------------------------------===//
// Utility: ensure directory exists
//===--------------------------------------------------------------------===//

static void EnsureDir(FileSystem &fs, const std::string &dir) {
    if (!fs.DirectoryExists(dir)) {
        fs.CreateDirectory(dir);
    }
}

//===--------------------------------------------------------------------===//
// mwdb_create(db_path VARCHAR) -> VARCHAR
//===--------------------------------------------------------------------===//

static void MWDBCreateImpl(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    auto &fs = FileSystem::GetFileSystem(context);
    idx_t count = args.size();

    for (idx_t i = 0; i < count; i++) {
        string db_path = args.data[0].GetValue(i).ToString();

        EnsureDir(fs, db_path);
        EnsureDir(fs, MWDBIndexBin::DataDir(db_path));

        // Only create _index.bin if it doesn't already exist
        string bin_path = MWDBIndexBin::IndexBinPath(db_path);
        if (!fs.FileExists(bin_path)) {
            auto idx = MWDBIndexBin::Create();
            idx.Save(db_path);
        }

        result.SetValue(i, Value("Created multiwriter database at: " + db_path));
    }
}

void RegisterCreateFunctions(ExtensionLoader &loader) {
    ScalarFunction create_fn("mwdb_create",
                              {LogicalType::VARCHAR},
                              LogicalType::VARCHAR,
                              MWDBCreateImpl);
    loader.RegisterFunction(create_fn);
}

//===--------------------------------------------------------------------===//
// mwdb_create_table(db_path, name, schema_sql) -> VARCHAR
//===--------------------------------------------------------------------===//

static void MWDBCreateTableImpl(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    auto &fs = FileSystem::GetFileSystem(context);
    idx_t count = args.size();

    for (idx_t i = 0; i < count; i++) {
        string db_path   = args.data[0].GetValue(i).ToString();
        string tbl_name  = args.data[1].GetValue(i).ToString();
        string schema    = args.data[2].GetValue(i).ToString();

        // Parse schema using DuckDB parser
        string full_sql = "CREATE TABLE _mwdb_t_(" + schema + ")";
        Parser parser;
        try {
            parser.ParseQuery(full_sql);
        } catch (std::exception &e) {
            throw InvalidInputException("mwdb_create_table: failed to parse schema: %s", e.what());
        }
        if (parser.statements.empty()) {
            throw InvalidInputException("mwdb_create_table: empty schema");
        }

        auto &stmt = *parser.statements[0];
        if (stmt.type != StatementType::CREATE_STATEMENT) {
            throw InvalidInputException("mwdb_create_table: expected CREATE TABLE");
        }
        auto &create_stmt = stmt.Cast<CreateStatement>();
        if (create_stmt.info->type != CatalogType::TABLE_ENTRY) {
            throw InvalidInputException("mwdb_create_table: expected TABLE entry");
        }
        auto &tinfo = create_stmt.info->Cast<CreateTableInfo>();

        // Find primary key columns from constraints
        std::set<string> pk_col_names;
        for (auto &con : tinfo.constraints) {
            if (con->type == ConstraintType::UNIQUE) {
                auto &ucon = con->Cast<UniqueConstraint>();
                if (ucon.IsPrimaryKey()) {
                    if (ucon.HasIndex()) {
                        // Single-column: get by logical index
                        auto &col = tinfo.columns.GetColumn(ucon.GetIndex());
                        pk_col_names.insert(col.Name());
                    } else {
                        // Multi-column
                        for (auto &cn : ucon.GetColumnNames()) {
                            pk_col_names.insert(cn);
                        }
                    }
                }
            }
        }

        // Build MWDBTableSection
        MWDBTableSection ts;
        ts.version = 1;
        uint16_t col_id = 0;
        for (idx_t c = 0; c < tinfo.columns.LogicalColumnCount(); c++) {
            auto &col_def = tinfo.columns.GetColumn(LogicalIndex(c));
            MWDBColumnInfo ci;
            ci.col_id   = col_id++;
            ci.name     = col_def.Name();
            ci.type     = col_def.Type();
            ci.nullable = true; // default nullable
            ci.is_pk    = (pk_col_names.count(col_def.Name()) > 0);
            if (ci.is_pk) {
                ts.pk_col_ids.push_back(ci.col_id);
            }
            ts.columns.push_back(std::move(ci));
        }
        ts.has_pk_index = false;

        // Load index, add table, save
        auto idx = MWDBIndexBin::Load(db_path);
        idx.AddTable(tbl_name, ts);
        idx.Save(db_path);

        // Create table data directory
        string tbl_dir = MWDBIndexBin::TableDataDir(db_path, tbl_name);
        EnsureDir(fs, tbl_dir);

        result.SetValue(i, Value("Created table '" + tbl_name + "' in " + db_path));
    }
}

void RegisterWriteFunction(ExtensionLoader &loader);  // forward declaration

void RegisterCreateTableFunction(ExtensionLoader &loader) {
    ScalarFunction fn("mwdb_create_table",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                       LogicalType::VARCHAR,
                       MWDBCreateTableImpl);
    loader.RegisterFunction(fn);
}

//===--------------------------------------------------------------------===//
// mwdb_write(db_path, table, query_sql)
//   -> STRUCT(rows_written BIGINT, segment VARCHAR)
//===--------------------------------------------------------------------===//

static LogicalType MWDBWriteReturnType() {
    child_list_t<LogicalType> children;
    children.emplace_back("rows_written", LogicalType::BIGINT);
    children.emplace_back("segment",      LogicalType::VARCHAR);
    return LogicalType::STRUCT(children);
}

static void MWDBWriteImpl(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    auto &fs = FileSystem::GetFileSystem(context);
    idx_t count = args.size();

    for (idx_t i = 0; i < count; i++) {
        string db_path   = args.data[0].GetValue(i).ToString();
        string tbl_name  = args.data[1].GetValue(i).ToString();
        string query_sql = args.data[2].GetValue(i).ToString();

        // Generate UUID for the new segment
        auto uuid     = MWDBIndexBin::GenerateUUID();
        string uuid_s = MWDBIndexBin::UUIDToString(uuid);

        // Determine absolute parquet file path
        string tbl_dir     = MWDBIndexBin::TableDataDir(db_path, tbl_name);
        EnsureDir(fs, tbl_dir);
        string parquet_path = tbl_dir + "/" + uuid_s + ".parquet";

        // Relative path stored in the index
        string rel_path = "_data/" + tbl_name + "/" + uuid_s + ".parquet";

        // Execute COPY via a new connection (auto-committed transaction)
        Connection conn(*context.db);
        string copy_sql = "COPY (" + query_sql + ") TO '" + parquet_path +
                          "' (FORMAT PARQUET)";
        auto copy_result = conn.Query(copy_sql);
        if (copy_result->HasError()) {
            throw IOException("mwdb_write COPY failed: %s", copy_result->GetError());
        }

        // COPY TO returns a single row with column "Count" (BIGINT)
        int64_t rows_written = 0;
        if (copy_result->RowCount() > 0) {
            rows_written = copy_result->GetValue(0, 0).GetValue<int64_t>();
        }

        uint64_t file_size = GetFileSize(parquet_path);

        // Build segment entry
        MWDBSegmentEntry seg;
        seg.uuid       = uuid;
        seg.path       = rel_path;
        seg.row_count  = (uint64_t)rows_written;
        seg.file_size  = file_size;
        seg.written_at = NowMs();
        seg.state      = MWDBSegmentState::LIVE;

        // Update index
        auto idx = MWDBIndexBin::Load(db_path);
        idx.AddSegment(tbl_name, std::move(seg));
        idx.Save(db_path);

        // Return struct
        child_list_t<Value> struct_vals;
        struct_vals.emplace_back("rows_written", Value::BIGINT(rows_written));
        struct_vals.emplace_back("segment",      Value(uuid_s));
        result.SetValue(i, Value::STRUCT(struct_vals));
    }
}

void RegisterWriteFunction(ExtensionLoader &loader) {
    ScalarFunction fn("mwdb_write",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                       MWDBWriteReturnType(),
                       MWDBWriteImpl);
    loader.RegisterFunction(fn);
}

//===--------------------------------------------------------------------===//
// mwdb_scan(db_path VARCHAR, table VARCHAR) -> TABLE
//===--------------------------------------------------------------------===//

struct MWScanBindData : public TableFunctionData {
    string db_path;
    string table_name;
    vector<string>      parquet_paths;   // absolute paths
    vector<string>      col_names;
    vector<LogicalType> col_types;
};

struct MWScanGlobalState : public GlobalTableFunctionState {
    unique_ptr<MaterializedQueryResult> result;
    ColumnDataScanState                 scan_state;
    bool                                initialized = false;
    bool                                done        = false;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> MWScanBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string>       &names) {
    auto bind_data = make_uniq<MWScanBindData>();
    bind_data->db_path    = input.inputs[0].ToString();
    bind_data->table_name = input.inputs[1].ToString();

    // Load the index to get column schema and live segment paths
    try {
        auto idx = MWDBIndexBin::Load(bind_data->db_path);
        auto *entry = idx.FindTable(bind_data->table_name);
        if (!entry) {
            throw InvalidInputException("mwdb_scan: table '%s' not found",
                                         bind_data->table_name.c_str());
        }
        auto &ts = entry->table_section;
        for (auto &col : ts.columns) {
            bind_data->col_names.push_back(col.name);
            bind_data->col_types.push_back(col.type);
        }
        for (auto *seg : ts.LiveSegments()) {
            // Convert relative path to absolute
            bind_data->parquet_paths.push_back(
                bind_data->db_path + "/" + seg->path);
        }
    } catch (std::exception &e) {
        throw IOException("mwdb_scan: cannot load index: %s", e.what());
    }

    return_types = bind_data->col_types;
    names        = bind_data->col_names;
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState>
MWScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<MWScanGlobalState>();
    auto &bind = input.bind_data->Cast<MWScanBindData>();

    if (bind.parquet_paths.empty()) {
        state->done = true;
        return std::move(state);
    }

    // Build read_parquet query
    string query;
    if (bind.parquet_paths.size() == 1) {
        query = "SELECT * FROM read_parquet('" + bind.parquet_paths[0] + "')";
    } else {
        query = "SELECT * FROM read_parquet([";
        for (size_t j = 0; j < bind.parquet_paths.size(); j++) {
            if (j > 0) query += ",";
            query += "'" + bind.parquet_paths[j] + "'";
        }
        query += "])";
    }

    Connection conn(*context.db);
    state->result = conn.Query(query);
    if (state->result->HasError()) {
        throw IOException("mwdb_scan: read_parquet failed: %s",
                           state->result->GetError().c_str());
    }

    state->result->Collection().InitializeScan(state->scan_state);
    state->initialized = true;
    return std::move(state);
}

static void MWScanFunction(ClientContext & /*context*/,
                            TableFunctionInput &data_p,
                            DataChunk &output) {
    auto &state = data_p.global_state->Cast<MWScanGlobalState>();
    if (state.done || !state.initialized) return;

    bool has_more = state.result->Collection().Scan(state.scan_state, output);
    if (!has_more) {
        state.done = true;
    }
}

void RegisterScanFunction(ExtensionLoader &loader) {
    TableFunction scan_fn("mwdb_scan",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR},
                           MWScanFunction,
                           MWScanBind,
                           MWScanInitGlobal);
    loader.RegisterFunction(scan_fn);
}

//===--------------------------------------------------------------------===//
// mwdb_lookup(db_path, table, pk_value VARCHAR) -> TABLE
// Simple implementation: filter all live segments for the PK value.
//===--------------------------------------------------------------------===//

struct MWLookupBindData : public TableFunctionData {
    string db_path;
    string table_name;
    string pk_value;
    string pk_col_name;
    vector<string>      parquet_paths;
    vector<string>      col_names;
    vector<LogicalType> col_types;
};

struct MWLookupGlobalState : public GlobalTableFunctionState {
    unique_ptr<MaterializedQueryResult> result;
    ColumnDataScanState                 scan_state;
    bool                                initialized = false;
    bool                                done        = false;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> MWLookupBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string>       &names) {
    auto bind_data = make_uniq<MWLookupBindData>();
    bind_data->db_path    = input.inputs[0].ToString();
    bind_data->table_name = input.inputs[1].ToString();
    bind_data->pk_value   = input.inputs[2].ToString();

    try {
        auto idx = MWDBIndexBin::Load(bind_data->db_path);
        auto *entry = idx.FindTable(bind_data->table_name);
        if (!entry) {
            throw InvalidInputException("mwdb_lookup: table '%s' not found",
                                         bind_data->table_name.c_str());
        }
        auto &ts = entry->table_section;
        for (auto &col : ts.columns) {
            bind_data->col_names.push_back(col.name);
            bind_data->col_types.push_back(col.type);
            if (col.is_pk && bind_data->pk_col_name.empty()) {
                bind_data->pk_col_name = col.name;
            }
        }
        for (auto *seg : ts.LiveSegments()) {
            bind_data->parquet_paths.push_back(
                bind_data->db_path + "/" + seg->path);
        }
    } catch (std::exception &e) {
        throw IOException("mwdb_lookup: cannot load index: %s", e.what());
    }

    if (bind_data->pk_col_name.empty() && !bind_data->col_names.empty()) {
        // Fall back to first column if no PK is designated
        bind_data->pk_col_name = bind_data->col_names[0];
    }

    return_types = bind_data->col_types;
    names        = bind_data->col_names;
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState>
MWLookupInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<MWLookupGlobalState>();
    auto &bind = input.bind_data->Cast<MWLookupBindData>();

    if (bind.parquet_paths.empty() || bind.pk_col_name.empty()) {
        state->done = true;
        return std::move(state);
    }

    // Build a filtered read_parquet query for each segment and union them
    string query;
    // Escape the pk_value to prevent injection (basic escaping)
    string escaped_val = bind.pk_value;
    // Replace ' with '' for SQL string literal safety
    size_t pos = 0;
    while ((pos = escaped_val.find('\'', pos)) != string::npos) {
        escaped_val.replace(pos, 1, "''");
        pos += 2;
    }

    if (bind.parquet_paths.size() == 1) {
        query = "SELECT * FROM read_parquet('" + bind.parquet_paths[0] + "') WHERE \"" +
                bind.pk_col_name + "\"::VARCHAR = '" + escaped_val + "'";
    } else {
        // Union all segments
        for (size_t j = 0; j < bind.parquet_paths.size(); j++) {
            if (j > 0) query += " UNION ALL ";
            query += "SELECT * FROM read_parquet('" + bind.parquet_paths[j] +
                     "') WHERE \"" + bind.pk_col_name + "\"::VARCHAR = '" + escaped_val + "'";
        }
    }

    Connection conn(*context.db);
    state->result = conn.Query(query);
    if (state->result->HasError()) {
        throw IOException("mwdb_lookup: query failed: %s",
                           state->result->GetError().c_str());
    }

    state->result->Collection().InitializeScan(state->scan_state);
    state->initialized = true;
    return std::move(state);
}

static void MWLookupFunction(ClientContext & /*context*/,
                              TableFunctionInput &data_p,
                              DataChunk &output) {
    auto &state = data_p.global_state->Cast<MWLookupGlobalState>();
    if (state.done || !state.initialized) return;

    bool has_more = state.result->Collection().Scan(state.scan_state, output);
    if (!has_more) {
        state.done = true;
    }
}

void RegisterLookupFunction(ExtensionLoader &loader) {
    TableFunction lookup_fn("mwdb_lookup",
                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                             MWLookupFunction,
                             MWLookupBind,
                             MWLookupInitGlobal);
    loader.RegisterFunction(lookup_fn);
}

//===--------------------------------------------------------------------===//
// mwdb_info(db_path VARCHAR) -> TABLE
//   table_name VARCHAR, entry_type VARCHAR, segment_count BIGINT,
//   live_segments BIGINT, row_count BIGINT, size_bytes BIGINT
//===--------------------------------------------------------------------===//

struct MWInfoBindData : public TableFunctionData {
    string db_path;
};

struct MWInfoGlobalState : public GlobalTableFunctionState {
    vector<MWDBRootEntry> entries;
    idx_t offset = 0;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> MWInfoBind(ClientContext & /*context*/,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string>       &names) {
    auto bind_data = make_uniq<MWInfoBindData>();
    bind_data->db_path = input.inputs[0].ToString();

    names        = {"table_name", "entry_type", "segment_count", "live_segments", "row_count", "size_bytes"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT,  LogicalType::BIGINT,
                    LogicalType::BIGINT,  LogicalType::BIGINT};
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState>
MWInfoInitGlobal(ClientContext & /*context*/, TableFunctionInitInput &input) {
    auto state = make_uniq<MWInfoGlobalState>();
    auto &bind = input.bind_data->Cast<MWInfoBindData>();

    try {
        auto idx = MWDBIndexBin::Load(bind.db_path);
        state->entries = idx.Entries();
    } catch (std::exception &e) {
        // Return empty on error
    }
    return std::move(state);
}

static void MWInfoFunction(ClientContext & /*context*/,
                            TableFunctionInput &data_p,
                            DataChunk &output) {
    auto &state = data_p.global_state->Cast<MWInfoGlobalState>();
    if (state.offset >= state.entries.size()) return;

    idx_t count = 0;
    idx_t max_out = STANDARD_VECTOR_SIZE;
    while (state.offset < state.entries.size() && count < max_out) {
        auto &e = state.entries[state.offset++];

        string entry_type = e.IsTable() ? "TABLE" : "VIEW";
        int64_t seg_count  = 0;
        int64_t live_segs  = 0;
        int64_t row_count  = 0;
        int64_t size_bytes = 0;

        if (e.IsTable()) {
            seg_count  = (int64_t)e.table_section.segments.size();
            live_segs  = (int64_t)e.table_section.LiveSegments().size();
            row_count  = (int64_t)e.row_count_cached;
            size_bytes = (int64_t)e.size_bytes_cached;
        }

        output.data[0].SetValue(count, Value(e.name));
        output.data[1].SetValue(count, Value(entry_type));
        output.data[2].SetValue(count, Value::BIGINT(seg_count));
        output.data[3].SetValue(count, Value::BIGINT(live_segs));
        output.data[4].SetValue(count, Value::BIGINT(row_count));
        output.data[5].SetValue(count, Value::BIGINT(size_bytes));
        count++;
    }
    output.SetCardinality(count);
}

void RegisterInfoFunction(ExtensionLoader &loader) {
    TableFunction info_fn("mwdb_info",
                           {LogicalType::VARCHAR},
                           MWInfoFunction,
                           MWInfoBind,
                           MWInfoInitGlobal);
    loader.RegisterFunction(info_fn);
}

//===--------------------------------------------------------------------===//
// Master registration
//===--------------------------------------------------------------------===//

void RegisterMWDBFunctions(ExtensionLoader &loader) {
    RegisterCreateFunctions(loader);
    RegisterCreateTableFunction(loader);
    RegisterWriteFunction(loader);
    RegisterScanFunction(loader);
    RegisterLookupFunction(loader);
    RegisterInfoFunction(loader);
}

} // namespace duckdb
