//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/multifile/multifile_storage.hpp
//
// Multi-writer storage extension — native SQL via ATTACH 'path/' (TYPE multifile).
//
// Design: each table's rows live in immutable Parquet segments on disk.
// An _index.bin file (binary, atomic-write) tracks the schema, live segments,
// zone maps, and optional PK index for every table in the database.
//
// On ATTACH  : read _index.bin → recreate tables in DuckCatalog (in-memory)
//              → bulk-load existing Parquet rows into DuckDB's buffer pool.
// On INSERT  : goes into DuckDB's native in-memory table (no code change).
// On CHECKPOINT / DETACH : write each table to a new Parquet segment
//              → atomically update _index.bin.
//
// Multi-writer safety (V1): each session writes only its own checkpoint.
// Two sessions open simultaneously will each produce their own segment file,
// so both checkpoints succeed independently. Full concurrent-read/write
// delta-tracking is a V2 enhancement.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/multifile/mwdb_format.hpp"
#include "duckdb/storage/multifile/mwdb_pk_index.hpp"

namespace duckdb {

class AttachedDatabase;
class ClientContext;
struct AttachInfo;
struct AttachOptions;

//===--------------------------------------------------------------------===//
// MultifileCatalog
//
// A DuckCatalog backed by :memory: storage but persisted to Parquet segments.
// FinalizeLoad recreates tables and loads existing data from _index.bin.
//===--------------------------------------------------------------------===//
class MultifileCatalog : public DuckCatalog {
public:
	MultifileCatalog(AttachedDatabase &db, string db_dir, string db_name);
	~MultifileCatalog() override = default;

	//------------------------------------------------------------------
	// Catalog interface
	//------------------------------------------------------------------
	void Initialize(bool load_builtin) override;
	void FinalizeLoad(optional_ptr<ClientContext> context) override;

	string GetCatalogType() override {
		return "multifile";
	}
	string GetDBPath() override {
		return db_dir_;
	}
	bool InMemory() override {
		// Not in-memory: data persists to Parquet on checkpoint.
		return false;
	}
	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//------------------------------------------------------------------
	// Accessors for the transaction manager
	//------------------------------------------------------------------
	const string &GetDBDir() const {
		return db_dir_;
	}
	const string &GetCatalogName() const {
		return db_name_;
	}
	MWDBIndexBin &GetIndexBin() {
		return index_bin_;
	}
	bool IsIndexLoaded() const {
		return index_loaded_;
	}

private:
	string         db_dir_;
	string         db_name_;
	MWDBIndexBin   index_bin_;
	bool           index_loaded_ = false;

	void LoadTablesFromIndex(ClientContext &context);
};

//===--------------------------------------------------------------------===//
// MultifileTransactionManager
//
// Extends DuckTransactionManager so DuckDB's internal IsDuckTransactionManager()
// check passes. Overrides only Checkpoint to write Parquet segments and update
// _index.bin; all other transaction operations are inherited unchanged.
//===--------------------------------------------------------------------===//
class MultifileTransactionManager : public DuckTransactionManager {
public:
	MultifileTransactionManager(AttachedDatabase &db, MultifileCatalog &catalog);
	~MultifileTransactionManager() override = default;

	// Writes each table to a Parquet segment and updates _index.bin,
	// then calls DuckTransactionManager::Checkpoint for WAL cleanup.
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	MultifileCatalog &catalog_;

	// Helper: ensure the per-table data directory exists.
	static void EnsureTableDir(FileSystem &fs, const string &db_dir, const string &table_name);

	// Helper: get current unix time in milliseconds.
	static uint64_t NowMs();
};

//===--------------------------------------------------------------------===//
// Factory
//===--------------------------------------------------------------------===//
struct MultifileStorageExtension {
	static shared_ptr<StorageExtension> Create();
};

} // namespace duckdb
