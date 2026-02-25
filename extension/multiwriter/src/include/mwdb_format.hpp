//===----------------------------------------------------------------------===//
// mwdb_format.hpp
// Binary _index.bin format for the multi-writer DuckDB extension.
//
// Layout (all offsets absolute):
//   [Header 128B][String Table][Root Directory][Table Sections...]
//
// All integers little-endian. Strings: length-prefixed (u32 + bytes).
// Atomic writes: write to tmp file, fsync, rename.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"

#include <cstdint>
#include <string>
#include <vector>
#include <array>
#include <optional>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constants
//===--------------------------------------------------------------------===//

static constexpr uint64_t MWDB_MAGIC          = 0x4244574D4C444E49ULL; // "INDLMWDB"
static constexpr uint16_t MWDB_FORMAT_VERSION = 1;
static constexpr uint16_t MWDB_MIN_READER_VER = 1;
static constexpr uint32_t MWDB_NO_BUCKET      = 0xFFFFFFFFU;
static constexpr uint32_t MWDB_NO_OFFSET      = 0U;

//===--------------------------------------------------------------------===//
// Enums
//===--------------------------------------------------------------------===//

enum class MWDBEntryType : uint8_t {
    TABLE = 0,
    VIEW  = 1,
};

enum class MWDBSegmentState : uint8_t {
    LIVE     = 0, // segment is active and queryable
    OBSOLETE = 1, // replaced by compaction, pending vacuum
};

//===--------------------------------------------------------------------===//
// Zone map for a single column within a segment
//===--------------------------------------------------------------------===//

struct MWDBZoneMap {
    uint16_t col_id     = 0;
    uint64_t null_count = 0;
    bool     has_minmax = false;
    Value    min_val;   // duckdb Value (handles any type)
    Value    max_val;
};

//===--------------------------------------------------------------------===//
// A segment = one immutable Parquet file on disk
//===--------------------------------------------------------------------===//

struct MWDBSegmentEntry {
    std::array<uint8_t, 16> uuid   = {};   // random 16 bytes
    std::string             path;          // relative to db_dir
    uint64_t                row_count  = 0;
    uint64_t                file_size  = 0;
    uint64_t                written_at = 0; // unix ms
    uint32_t                bucket_id  = MWDB_NO_BUCKET;
    uint16_t                generation = 0;
    MWDBSegmentState        state      = MWDBSegmentState::LIVE;
    uint32_t                flags      = 0;

    std::vector<MWDBZoneMap> zone_maps; // one per column
};

//===--------------------------------------------------------------------===//
// Column descriptor
//===--------------------------------------------------------------------===//

struct MWDBColumnInfo {
    uint16_t    col_id   = 0;
    std::string name;
    LogicalType type     = LogicalType::INTEGER;
    bool        nullable = true;
    bool        is_pk    = false;    // part of primary key
};

//===--------------------------------------------------------------------===//
// Per-table section (loaded lazily from _index.bin)
//===--------------------------------------------------------------------===//

struct MWDBTableSection {
    uint64_t table_id = 0;
    uint32_t version  = 1;

    std::vector<MWDBColumnInfo>  columns;
    std::vector<uint16_t>        pk_col_ids;    // primary key column indices
    std::vector<uint16_t>        cluster_col_ids;

    std::vector<MWDBSegmentEntry> segments;

    // Feature flags stored in section header
    bool has_pk_index     = false; // global PK index present
    bool has_bloom        = false; // bloom filters present
    bool has_partitioning = false; // partition info present

    // Helpers
    bool HasPrimaryKey() const { return !pk_col_ids.empty(); }

    std::vector<MWDBSegmentEntry*> LiveSegments() {
        std::vector<MWDBSegmentEntry*> live;
        for (auto &seg : segments) {
            if (seg.state == MWDBSegmentState::LIVE) {
                live.push_back(&seg);
            }
        }
        return live;
    }
};

//===--------------------------------------------------------------------===//
// Root directory entry
//===--------------------------------------------------------------------===//

struct MWDBRootEntry {
    MWDBEntryType type     = MWDBEntryType::TABLE;
    uint64_t      table_id = 0;
    std::string   name;

    // TABLE fields
    uint64_t row_count_cached  = 0;
    uint64_t size_bytes_cached = 0;
    bool     section_loaded    = false;
    MWDBTableSection table_section;

    // VIEW fields
    std::string view_sql;

    bool IsTable() const { return type == MWDBEntryType::TABLE; }
    bool IsView()  const { return type == MWDBEntryType::VIEW;  }
};

//===--------------------------------------------------------------------===//
// The in-memory representation of an entire _index.bin
//===--------------------------------------------------------------------===//

class MWDBIndexBin {
public:
    //------------------------------------------------------------------
    // Factory
    //------------------------------------------------------------------
    static MWDBIndexBin Create();                    // blank new database
    static MWDBIndexBin Load(const std::string &db_dir);  // read from disk

    //------------------------------------------------------------------
    // Persistence  (atomic: write tmp → fsync → rename)
    //------------------------------------------------------------------
    void Save(const std::string &db_dir) const;

    //------------------------------------------------------------------
    // Table management
    //------------------------------------------------------------------
    bool                TableExists(const std::string &name) const;
    MWDBRootEntry      *FindTable(const std::string &name);
    const MWDBRootEntry*FindTable(const std::string &name) const;

    // Returns new table_id
    uint64_t AddTable(const std::string &name, const MWDBTableSection &section);

    // View management
    void AddView(const std::string &name, const std::string &sql);
    bool ViewExists(const std::string &name) const;

    // Segment management
    void AddSegment(const std::string &table_name, MWDBSegmentEntry seg);
    void MarkSegmentObsolete(const std::string &table_name, const std::array<uint8_t,16> &uuid);

    //------------------------------------------------------------------
    // Root directory access
    //------------------------------------------------------------------
    std::vector<MWDBRootEntry> &Entries() { return root_dir_; }
    const std::vector<MWDBRootEntry> &Entries() const { return root_dir_; }

    //------------------------------------------------------------------
    // Utility
    //------------------------------------------------------------------
    static std::string IndexBinPath(const std::string &db_dir);
    static std::string DataDir(const std::string &db_dir);
    static std::string TableDataDir(const std::string &db_dir, const std::string &table_name);
    static std::array<uint8_t,16> GenerateUUID();
    static std::string UUIDToString(const std::array<uint8_t,16> &uuid);

private:
    uint64_t next_table_id_ = 1;
    std::vector<MWDBRootEntry> root_dir_;

    // Serialization helpers
    static void WriteU8 (std::vector<uint8_t> &buf, uint8_t  v);
    static void WriteU16(std::vector<uint8_t> &buf, uint16_t v);
    static void WriteU32(std::vector<uint8_t> &buf, uint32_t v);
    static void WriteU64(std::vector<uint8_t> &buf, uint64_t v);
    static void WriteStr(std::vector<uint8_t> &buf, const std::string &s);
    static void WriteBytes(std::vector<uint8_t> &buf, const uint8_t *data, size_t len);

    static uint8_t  ReadU8 (const uint8_t *&p, const uint8_t *end);
    static uint16_t ReadU16(const uint8_t *&p, const uint8_t *end);
    static uint32_t ReadU32(const uint8_t *&p, const uint8_t *end);
    static uint64_t ReadU64(const uint8_t *&p, const uint8_t *end);
    static std::string ReadStr(const uint8_t *&p, const uint8_t *end);

    static void SerializeZoneMaps(std::vector<uint8_t> &buf,
                                  const std::vector<MWDBZoneMap> &zmaps);
    static std::vector<MWDBZoneMap> DeserializeZoneMaps(const uint8_t *&p,
                                                         const uint8_t *end);

    static void SerializeType(std::vector<uint8_t> &buf, const LogicalType &t);
    static LogicalType DeserializeType(const uint8_t *&p, const uint8_t *end);

    static void SerializeValue(std::vector<uint8_t> &buf, const Value &v);
    static Value DeserializeValue(const uint8_t *&p, const uint8_t *end,
                                  const LogicalType &t);

    std::vector<uint8_t> Serialize() const;
    void Deserialize(const std::vector<uint8_t> &data);
};

} // namespace duckdb
