#pragma once
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// mwdb_create(db_path)           → VARCHAR  (confirmation message)
// mwdb_create_table(db_path, name, schema_sql) → VARCHAR
// mwdb_write(db_path, table, query_sql)         → STRUCT(rows_written BIGINT, segment VARCHAR)
// mwdb_scan(db_path, table)      → TABLE   (all rows from all live segments)
// mwdb_lookup(db_path, table, pk_value)         → TABLE   (point lookup via PK index)
// mwdb_info(db_path)             → TABLE   (shows tables, segments, stats)

void RegisterMWDBFunctions(ExtensionLoader &loader);

// Individual registrations (called by RegisterMWDBFunctions)
void RegisterCreateFunctions(ExtensionLoader &loader);
void RegisterWriteFunction(ExtensionLoader &loader);
void RegisterScanFunction(ExtensionLoader &loader);
void RegisterLookupFunction(ExtensionLoader &loader);
void RegisterInfoFunction(ExtensionLoader &loader);

} // namespace duckdb
