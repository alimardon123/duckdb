//===----------------------------------------------------------------------===//
// multifile_storage.cpp
//
// Native multi-writer storage extension for DuckDB.
//
// Registered in DBConfig as "multifile".  Users attach with:
//   ATTACH 'path/to/mydb.duckdb/' AS mydb (TYPE multifile);
//   CREATE TABLE mydb.orders (id INTEGER, amount DOUBLE);
//   INSERT INTO mydb.orders VALUES (1, 99.5);
//   SELECT * FROM mydb.orders WHERE amount > 50;
//
// Data layout on disk:
//   mydb.duckdb/
//     _index.bin          ← schema + segment registry (atomic replace)
//     _data/
//       orders/
//         <uuid>.parquet  ← immutable segment, written once
//===----------------------------------------------------------------------===//
#include "duckdb/storage/multifile/multifile_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <chrono>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

static void EnsureDir(FileSystem &fs, const string &dir) {
	if (!fs.DirectoryExists(dir)) {
		fs.CreateDirectory(dir);
	}
}

static string QuoteIdent(const string &s) {
	// Escape embedded double-quotes by doubling them.
	string out;
	out.reserve(s.size() + 2);
	out += '"';
	for (char c : s) {
		if (c == '"') {
			out += '"';
		}
		out += c;
	}
	out += '"';
	return out;
}

uint64_t MultifileTransactionManager::NowMs() {
	auto now = std::chrono::system_clock::now();
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
}

//===--------------------------------------------------------------------===//
// MultifileCatalog
//===--------------------------------------------------------------------===//

MultifileCatalog::MultifileCatalog(AttachedDatabase &db, string db_dir, string db_name)
    : DuckCatalog(db), db_dir_(std::move(db_dir)), db_name_(std::move(db_name)) {
}

void MultifileCatalog::Initialize(bool /*load_builtin*/) {
	// Initialise the in-memory DuckCatalog schema WITHOUT built-in functions.
	// Built-ins live in the system catalog and are available through catalog
	// search; we don't need them duplicated here.
	DuckCatalog::Initialize(false);
}

DatabaseSize MultifileCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	if (index_loaded_) {
		for (auto &e : index_bin_.Entries()) {
			if (e.IsTable()) {
				size.total_blocks += e.size_bytes_cached / 4096 + 1;
				size.used_blocks  += e.size_bytes_cached / 4096 + 1;
			}
		}
	}
	return size;
}

void MultifileCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
	DuckCatalog::FinalizeLoad(context);
	if (!context) {
		return;
	}
	LoadTablesFromIndex(*context);
}

void MultifileCatalog::LoadTablesFromIndex(ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Ensure the data directory exists.
	EnsureDir(fs, db_dir_ + "/_data");

	string bin_path = MWDBIndexBin::IndexBinPath(db_dir_);
	if (!fs.FileExists(bin_path)) {
		// Fresh database – nothing to load.
		return;
	}

	try {
		index_bin_     = MWDBIndexBin::Load(db_dir_);
		index_loaded_  = true;
	} catch (...) {
		// Corrupt or empty index → start fresh.
		return;
	}

	// Use a fresh Connection so we can run DDL/DML.
	// The attached database is already registered by the time FinalizeLoad
	// is called, so we can reference it by name.
	auto &db_instance = DatabaseInstance::GetDatabase(context);
	Connection conn(db_instance);

	for (auto &entry : index_bin_.Entries()) {
		if (entry.type != MWDBEntryType::TABLE) {
			continue;
		}
		auto &ts = entry.table_section;

		// ── Build CREATE TABLE SQL ───────────────────────────────────────
		string cols_sql;
		for (idx_t c = 0; c < ts.columns.size(); c++) {
			if (c > 0) {
				cols_sql += ", ";
			}
			auto &col = ts.columns[c];
			cols_sql += QuoteIdent(col.name) + " " + col.type.ToString();
		}

		string create_sql = "CREATE TABLE IF NOT EXISTS " + QuoteIdent(db_name_) +
		                    "." + DEFAULT_SCHEMA + "." + QuoteIdent(entry.name) +
		                    " (" + cols_sql + ")";
		auto create_res = conn.Query(create_sql);
		if (create_res->HasError()) {
			// Table may already exist from a previous call; continue.
			continue;
		}

		// ── Load rows from live Parquet segments ─────────────────────────
		auto live_segs = ts.LiveSegments();
		if (live_segs.empty()) {
			continue;
		}

		string paths;
		for (idx_t i = 0; i < live_segs.size(); i++) {
			if (i > 0) {
				paths += ", ";
			}
			paths += "'" + db_dir_ + "/" + live_segs[i]->path + "'";
		}

		string insert_sql = "INSERT INTO " + QuoteIdent(db_name_) +
		                    "." + DEFAULT_SCHEMA + "." + QuoteIdent(entry.name) +
		                    " SELECT * FROM read_parquet([" + paths + "])";
		auto insert_res = conn.Query(insert_sql);
		// Silently ignore errors (e.g. parquet extension not loaded).
		(void)insert_res;
	}
}

//===--------------------------------------------------------------------===//
// MultifileTransactionManager
//===--------------------------------------------------------------------===//

MultifileTransactionManager::MultifileTransactionManager(AttachedDatabase &db, MultifileCatalog &catalog)
    : DuckTransactionManager(db), catalog_(catalog) {
}

void MultifileTransactionManager::EnsureTableDir(FileSystem &fs, const string &db_dir, const string &table_name) {
	EnsureDir(fs, db_dir + "/_data");
	EnsureDir(fs, db_dir + "/_data/" + table_name);
}

void MultifileTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// 1. Flush the DuckDB in-memory WAL state (inherited behaviour).
	DuckTransactionManager::Checkpoint(context, force);

	auto &fs      = FileSystem::GetFileSystem(context);
	auto &db_dir  = catalog_.GetDBDir();
	auto &db_name = catalog_.GetCatalogName();

	EnsureDir(fs, db_dir);
	EnsureDir(fs, db_dir + "/_data");

	// 2. Open a connection to enumerate tables and copy data to Parquet.
	auto &db_instance = context.db;
	Connection conn(*db_instance);

	// Collect all user tables from the multifile catalog.
	vector<string> table_names;
	catalog_.ScanSchemas(*conn.context, [&](SchemaCatalogEntry &schema) {
		schema.Scan(*conn.context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal) {
				table_names.push_back(entry.name);
			}
		});
	});

	if (table_names.empty()) {
		// Nothing to persist.
		MWDBIndexBin empty = MWDBIndexBin::Create();
		empty.Save(db_dir);
		if (catalog_.IsIndexLoaded()) {
			catalog_.GetIndexBin() = std::move(empty);
		}
		return;
	}

	// 3. Build the new _index.bin in memory before touching disk.
	MWDBIndexBin new_index = MWDBIndexBin::Create();

	for (auto &tbl_name : table_names) {
		// ── Get row count ────────────────────────────────────────────────
		auto count_res = conn.Query("SELECT COUNT(*) FROM " + QuoteIdent(db_name) +
		                            "." + DEFAULT_SCHEMA + "." + QuoteIdent(tbl_name));
		if (count_res->HasError()) {
			continue;
		}
		int64_t row_count = count_res->GetValue(0, 0).GetValue<int64_t>();

		// ── Get table schema from catalog ────────────────────────────────
		auto entry_ptr =
		    catalog_.GetEntry<TableCatalogEntry>(*conn.context, DEFAULT_SCHEMA, tbl_name, OnEntryNotFound::RETURN_NULL);
		if (!entry_ptr) {
			continue;
		}
		auto &tbl_entry = *entry_ptr;

		MWDBTableSection ts;
		ts.version = 1;
		uint16_t col_id = 0;
		for (idx_t c = 0; c < tbl_entry.GetColumns().LogicalColumnCount(); c++) {
			auto &col_def = tbl_entry.GetColumns().GetColumn(LogicalIndex(c));
			// Skip generated columns.
			if (col_def.Generated()) {
				continue;
			}
			MWDBColumnInfo ci;
			ci.col_id   = col_id++;
			ci.name     = col_def.Name();
			ci.type     = col_def.Type();
			ci.nullable = true; // V1: no NOT NULL tracking
			ts.columns.push_back(ci);
		}

		new_index.AddTable(tbl_name, ts);

		if (row_count == 0) {
			// Empty table: register schema only, no segment file.
			continue;
		}

		// ── Write Parquet segment ────────────────────────────────────────
		EnsureTableDir(fs, db_dir, tbl_name);

		auto uuid     = MWDBIndexBin::GenerateUUID();
		auto uuid_str = MWDBIndexBin::UUIDToString(uuid);
		string seg_abs = db_dir + "/_data/" + tbl_name + "/" + uuid_str + ".parquet";
		string seg_rel = "_data/" + tbl_name + "/" + uuid_str + ".parquet";

		string copy_sql = "COPY " + QuoteIdent(db_name) + "." + DEFAULT_SCHEMA +
		                  "." + QuoteIdent(tbl_name) +
		                  " TO '" + seg_abs + "' (FORMAT PARQUET)";
		auto copy_res = conn.Query(copy_sql);
		if (copy_res->HasError()) {
			// parquet extension not loaded or other error; skip this table.
			continue;
		}

		// ── Collect file size for the index entry ────────────────────────
		uint64_t file_size = 0;
		try {
			auto fh   = fs.OpenFile(seg_abs, FileFlags::FILE_FLAGS_READ);
			file_size = static_cast<uint64_t>(fs.GetFileSize(*fh));
		} catch (...) {
		}

		// ── Register the segment ─────────────────────────────────────────
		MWDBSegmentEntry seg;
		seg.uuid       = uuid;
		seg.path       = seg_rel;
		seg.row_count  = static_cast<uint64_t>(row_count);
		seg.file_size  = file_size;
		seg.written_at = NowMs();
		seg.state      = MWDBSegmentState::LIVE;
		new_index.AddSegment(tbl_name, seg);
	}

	// 4. Atomically replace _index.bin.
	new_index.Save(db_dir);
	catalog_.GetIndexBin() = std::move(new_index);
}

//===--------------------------------------------------------------------===//
// StorageExtension attach / create_transaction_manager
//===--------------------------------------------------------------------===//

static unique_ptr<Catalog> MultifileAttach(optional_ptr<StorageExtensionInfo> /*info*/,
                                           ClientContext &context,
                                           AttachedDatabase &db,
                                           const string &name,
                                           AttachInfo &attach_info,
                                           AttachOptions & /*options*/) {
	// Strip trailing slashes from the directory path.
	string db_dir = attach_info.path;
	while (!db_dir.empty() && (db_dir.back() == '/' || db_dir.back() == '\\')) {
		db_dir.pop_back();
	}

	// Ensure the database directory and data subdirectory exist.
	auto &fs = FileSystem::GetFileSystem(context);
	EnsureDir(fs, db_dir);
	EnsureDir(fs, db_dir + "/_data");

	// Create _index.bin for a brand-new database.
	string bin_path = MWDBIndexBin::IndexBinPath(db_dir);
	if (!fs.FileExists(bin_path)) {
		auto idx = MWDBIndexBin::Create();
		idx.Save(db_dir);
	}

	// Set the backing path to :memory: so DuckDB creates an in-memory
	// SingleFileStorageManager for the schema/WAL layer.
	attach_info.path = ":memory:";

	return make_uniq<MultifileCatalog>(db, std::move(db_dir), name);
}

static unique_ptr<TransactionManager>
MultifileCreateTransactionManager(optional_ptr<StorageExtensionInfo> /*info*/,
                                  AttachedDatabase &db,
                                  Catalog &catalog) {
	auto &mf_catalog = catalog.Cast<MultifileCatalog>();
	return make_uniq<MultifileTransactionManager>(db, mf_catalog);
}

shared_ptr<StorageExtension> MultifileStorageExtension::Create() {
	auto ext                          = make_shared_ptr<StorageExtension>();
	ext->attach                       = MultifileAttach;
	ext->create_transaction_manager   = MultifileCreateTransactionManager;
	return ext;
}

} // namespace duckdb
