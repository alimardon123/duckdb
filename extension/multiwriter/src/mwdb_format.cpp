//===----------------------------------------------------------------------===//
// mwdb_format.cpp  –  _index.bin serialisation / deserialisation
//===----------------------------------------------------------------------===//
#include "mwdb_format.hpp"
#include "mwdb_pk_index.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include <chrono>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <stdexcept>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Path helpers
//===--------------------------------------------------------------------===//

std::string MWDBIndexBin::IndexBinPath(const std::string &db_dir) {
    return db_dir + "/_index.bin";
}

std::string MWDBIndexBin::DataDir(const std::string &db_dir) {
    return db_dir + "/_data";
}

std::string MWDBIndexBin::TableDataDir(const std::string &db_dir,
                                       const std::string &table_name) {
    return DataDir(db_dir) + "/" + table_name;
}

//===--------------------------------------------------------------------===//
// UUID generation / formatting
//===--------------------------------------------------------------------===//

std::array<uint8_t,16> MWDBIndexBin::GenerateUUID() {
    static std::mt19937_64 rng(std::random_device{}());
    std::array<uint8_t,16> uuid;
    uint64_t hi = rng(), lo = rng();
    memcpy(uuid.data(),   &hi, 8);
    memcpy(uuid.data()+8, &lo, 8);
    // Set version 4 and variant bits
    uuid[6] = (uuid[6] & 0x0F) | 0x40;
    uuid[8] = (uuid[8] & 0x3F) | 0x80;
    return uuid;
}

std::string MWDBIndexBin::UUIDToString(const std::array<uint8_t,16> &uuid) {
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) ss << '-';
        ss << std::setw(2) << (int)uuid[i];
    }
    return ss.str();
}

//===--------------------------------------------------------------------===//
// Low-level serialisation helpers
//===--------------------------------------------------------------------===//

void MWDBIndexBin::WriteU8(std::vector<uint8_t> &b, uint8_t v) {
    b.push_back(v);
}

void MWDBIndexBin::WriteU16(std::vector<uint8_t> &b, uint16_t v) {
    b.push_back(v & 0xFF);
    b.push_back((v >> 8) & 0xFF);
}

void MWDBIndexBin::WriteU32(std::vector<uint8_t> &b, uint32_t v) {
    b.push_back( v        & 0xFF);
    b.push_back((v >>  8) & 0xFF);
    b.push_back((v >> 16) & 0xFF);
    b.push_back((v >> 24) & 0xFF);
}

void MWDBIndexBin::WriteU64(std::vector<uint8_t> &b, uint64_t v) {
    for (int i = 0; i < 8; ++i) b.push_back((v >> (8*i)) & 0xFF);
}

void MWDBIndexBin::WriteStr(std::vector<uint8_t> &b, const std::string &s) {
    WriteU32(b, (uint32_t)s.size());
    b.insert(b.end(), s.begin(), s.end());
}

void MWDBIndexBin::WriteBytes(std::vector<uint8_t> &b, const uint8_t *data, size_t len) {
    WriteU32(b, (uint32_t)len);
    b.insert(b.end(), data, data + len);
}

// Read helpers – advance pointer, throw on overflow
static void CheckRemain(const uint8_t *p, const uint8_t *end, size_t need) {
    if ((size_t)(end - p) < need)
        throw std::runtime_error("_index.bin: unexpected end of data");
}

uint8_t MWDBIndexBin::ReadU8(const uint8_t *&p, const uint8_t *end) {
    CheckRemain(p, end, 1); return *p++;
}
uint16_t MWDBIndexBin::ReadU16(const uint8_t *&p, const uint8_t *end) {
    CheckRemain(p, end, 2);
    uint16_t v = (uint16_t)p[0] | ((uint16_t)p[1] << 8); p += 2; return v;
}
uint32_t MWDBIndexBin::ReadU32(const uint8_t *&p, const uint8_t *end) {
    CheckRemain(p, end, 4);
    uint32_t v = (uint32_t)p[0] | ((uint32_t)p[1]<<8) |
                 ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
    p += 4; return v;
}
uint64_t MWDBIndexBin::ReadU64(const uint8_t *&p, const uint8_t *end) {
    CheckRemain(p, end, 8);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= ((uint64_t)p[i] << (8*i));
    p += 8; return v;
}
std::string MWDBIndexBin::ReadStr(const uint8_t *&p, const uint8_t *end) {
    uint32_t len = ReadU32(p, end);
    CheckRemain(p, end, len);
    std::string s((const char*)p, len); p += len; return s;
}

//===--------------------------------------------------------------------===//
// LogicalType round-trip serialisation
// We encode as a compact type tag + optional parameters.
//===--------------------------------------------------------------------===//

// Tags must be stable across versions; extend by adding new values.
enum class TypeTag : uint8_t {
    BOOLEAN  =  0,
    TINYINT  =  1, SMALLINT =  2, INTEGER = 3, BIGINT   = 4,
    UTINYINT =  5, USMALLINT=  6, UINTEGER= 7, UBIGINT  = 8,
    FLOAT    =  9, DOUBLE   = 10,
    VARCHAR  = 11, BLOB     = 12,
    DATE     = 13, TIMESTAMP= 14, TIME     = 15,
    HUGEINT  = 16, UHUGEINT = 17,
    UNKNOWN  = 255,
};

void MWDBIndexBin::SerializeType(std::vector<uint8_t> &b, const LogicalType &t) {
    auto id = t.id();
    switch (id) {
        case LogicalTypeId::BOOLEAN:   WriteU8(b, (uint8_t)TypeTag::BOOLEAN);   return;
        case LogicalTypeId::TINYINT:   WriteU8(b, (uint8_t)TypeTag::TINYINT);   return;
        case LogicalTypeId::SMALLINT:  WriteU8(b, (uint8_t)TypeTag::SMALLINT);  return;
        case LogicalTypeId::INTEGER:   WriteU8(b, (uint8_t)TypeTag::INTEGER);   return;
        case LogicalTypeId::BIGINT:    WriteU8(b, (uint8_t)TypeTag::BIGINT);    return;
        case LogicalTypeId::UTINYINT:  WriteU8(b, (uint8_t)TypeTag::UTINYINT);  return;
        case LogicalTypeId::USMALLINT: WriteU8(b, (uint8_t)TypeTag::USMALLINT); return;
        case LogicalTypeId::UINTEGER:  WriteU8(b, (uint8_t)TypeTag::UINTEGER);  return;
        case LogicalTypeId::UBIGINT:   WriteU8(b, (uint8_t)TypeTag::UBIGINT);   return;
        case LogicalTypeId::FLOAT:     WriteU8(b, (uint8_t)TypeTag::FLOAT);     return;
        case LogicalTypeId::DOUBLE:    WriteU8(b, (uint8_t)TypeTag::DOUBLE);    return;
        case LogicalTypeId::VARCHAR:   WriteU8(b, (uint8_t)TypeTag::VARCHAR);   return;
        case LogicalTypeId::BLOB:      WriteU8(b, (uint8_t)TypeTag::BLOB);      return;
        case LogicalTypeId::DATE:      WriteU8(b, (uint8_t)TypeTag::DATE);      return;
        case LogicalTypeId::TIMESTAMP: WriteU8(b, (uint8_t)TypeTag::TIMESTAMP); return;
        case LogicalTypeId::TIME:      WriteU8(b, (uint8_t)TypeTag::TIME);      return;
        case LogicalTypeId::HUGEINT:   WriteU8(b, (uint8_t)TypeTag::HUGEINT);   return;
        case LogicalTypeId::UHUGEINT:  WriteU8(b, (uint8_t)TypeTag::UHUGEINT);  return;
        default:                       WriteU8(b, (uint8_t)TypeTag::UNKNOWN);   return;
    }
}

LogicalType MWDBIndexBin::DeserializeType(const uint8_t *&p, const uint8_t *end) {
    uint8_t tag = ReadU8(p, end);
    switch ((TypeTag)tag) {
        case TypeTag::BOOLEAN:   return LogicalType::BOOLEAN;
        case TypeTag::TINYINT:   return LogicalType::TINYINT;
        case TypeTag::SMALLINT:  return LogicalType::SMALLINT;
        case TypeTag::INTEGER:   return LogicalType::INTEGER;
        case TypeTag::BIGINT:    return LogicalType::BIGINT;
        case TypeTag::UTINYINT:  return LogicalType::UTINYINT;
        case TypeTag::USMALLINT: return LogicalType::USMALLINT;
        case TypeTag::UINTEGER:  return LogicalType::UINTEGER;
        case TypeTag::UBIGINT:   return LogicalType::UBIGINT;
        case TypeTag::FLOAT:     return LogicalType::FLOAT;
        case TypeTag::DOUBLE:    return LogicalType::DOUBLE;
        case TypeTag::VARCHAR:   return LogicalType::VARCHAR;
        case TypeTag::BLOB:      return LogicalType::BLOB;
        case TypeTag::DATE:      return LogicalType::DATE;
        case TypeTag::TIMESTAMP: return LogicalType::TIMESTAMP;
        case TypeTag::TIME:      return LogicalType::TIME;
        case TypeTag::HUGEINT:   return LogicalType::HUGEINT;
        case TypeTag::UHUGEINT:  return LogicalType::UHUGEINT;
        default:                 return LogicalType::VARCHAR; // best-effort fallback
    }
}

//===--------------------------------------------------------------------===//
// Value serialisation (for zone maps)
//===--------------------------------------------------------------------===//

void MWDBIndexBin::SerializeValue(std::vector<uint8_t> &b, const Value &v) {
    if (v.IsNull()) { WriteU8(b, 1); return; }
    WriteU8(b, 0); // not null

    auto str = v.ToString();
    WriteStr(b, str);
}

Value MWDBIndexBin::DeserializeValue(const uint8_t *&p, const uint8_t *end,
                                     const LogicalType &t) {
    uint8_t is_null = ReadU8(p, end);
    if (is_null) return Value(t);

    std::string str = ReadStr(p, end);
    try {
        return Value(str).DefaultCastAs(t);
    } catch (...) {
        return Value(t); // fallback: null
    }
}

//===--------------------------------------------------------------------===//
// Zone map serialisation
//===--------------------------------------------------------------------===//

void MWDBIndexBin::SerializeZoneMaps(std::vector<uint8_t> &b,
                                     const std::vector<MWDBZoneMap> &zmaps) {
    WriteU32(b, (uint32_t)zmaps.size());
    for (auto &zm : zmaps) {
        WriteU16(b, zm.col_id);
        WriteU64(b, zm.null_count);
        WriteU8 (b, zm.has_minmax ? 1 : 0);
        if (zm.has_minmax) {
            SerializeType(b, zm.min_val.type());
            SerializeValue(b, zm.min_val);
            SerializeValue(b, zm.max_val);
        }
    }
}

std::vector<MWDBZoneMap>
MWDBIndexBin::DeserializeZoneMaps(const uint8_t *&p, const uint8_t *end) {
    uint32_t n = ReadU32(p, end);
    std::vector<MWDBZoneMap> result;
    result.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        MWDBZoneMap zm;
        zm.col_id     = ReadU16(p, end);
        zm.null_count = ReadU64(p, end);
        zm.has_minmax = ReadU8(p, end) != 0;
        if (zm.has_minmax) {
            auto type  = DeserializeType(p, end);
            zm.min_val = DeserializeValue(p, end, type);
            zm.max_val = DeserializeValue(p, end, type);
        }
        result.push_back(std::move(zm));
    }
    return result;
}

//===--------------------------------------------------------------------===//
// Full serialisation of an MWDBIndexBin
//
// Wire format:
//   [magic u64][format_version u16][min_reader_ver u16][next_table_id u64]
//   [num_entries u32]
//   for each entry:
//     [type u8][table_id u64][name str]
//     if TABLE:
//       [row_count u64][size_bytes u64]
//       [section_version u32]
//       [num_columns u32] per col: [col_id u16][name str][type][nullable u8][is_pk u8]
//       [num_pk_cols u32] per: [col_id u16]
//       [num_cluster_cols u32] per: [col_id u16]
//       [has_pk_index u8]
//       [num_segments u32] per segment:
//         [uuid 16B][path str][row_count u64][file_size u64][written_at u64]
//         [bucket_id u32][generation u16][state u8][flags u32]
//         zone maps (serialized block)
//       if has_pk_index:
//         [pk_index_size u32][pk_index_bytes...]
//     if VIEW:
//       [sql str]
//===--------------------------------------------------------------------===//

std::vector<uint8_t> MWDBIndexBin::Serialize() const {
    std::vector<uint8_t> b;
    b.reserve(4096);

    WriteU64(b, MWDB_MAGIC);
    WriteU16(b, MWDB_FORMAT_VERSION);
    WriteU16(b, MWDB_MIN_READER_VER);
    WriteU64(b, next_table_id_);
    WriteU32(b, (uint32_t)root_dir_.size());

    for (auto &e : root_dir_) {
        WriteU8 (b, (uint8_t)e.type);
        WriteU64(b, e.table_id);
        WriteStr(b, e.name);

        if (e.type == MWDBEntryType::TABLE) {
            const auto &ts = e.table_section;
            WriteU64(b, e.row_count_cached);
            WriteU64(b, e.size_bytes_cached);
            WriteU32(b, ts.version);

            // Columns
            WriteU32(b, (uint32_t)ts.columns.size());
            for (auto &col : ts.columns) {
                WriteU16(b, col.col_id);
                WriteStr(b, col.name);
                SerializeType(b, col.type);
                WriteU8(b, col.nullable ? 1 : 0);
                WriteU8(b, col.is_pk   ? 1 : 0);
            }

            // PK col IDs
            WriteU32(b, (uint32_t)ts.pk_col_ids.size());
            for (auto id : ts.pk_col_ids) WriteU16(b, id);

            // Cluster col IDs
            WriteU32(b, (uint32_t)ts.cluster_col_ids.size());
            for (auto id : ts.cluster_col_ids) WriteU16(b, id);

            // Feature flags
            WriteU8(b, ts.has_pk_index ? 1 : 0);

            // Segments
            WriteU32(b, (uint32_t)ts.segments.size());
            for (auto &seg : ts.segments) {
                b.insert(b.end(), seg.uuid.begin(), seg.uuid.end());
                WriteStr(b, seg.path);
                WriteU64(b, seg.row_count);
                WriteU64(b, seg.file_size);
                WriteU64(b, seg.written_at);
                WriteU32(b, seg.bucket_id);
                WriteU16(b, seg.generation);
                WriteU8 (b, (uint8_t)seg.state);
                WriteU32(b, seg.flags);
                SerializeZoneMaps(b, seg.zone_maps);
            }

            // PK index (optional — written only if has_pk_index)
            // The index is stored per-table after all segments.
            // We do NOT store it inline here; it is maintained separately
            // in the _pk_index.bin sidecar to avoid blocking reads.
            // (reserved for future: write inline bytes)
        } else if (e.type == MWDBEntryType::VIEW) {
            WriteStr(b, e.view_sql);
        }
    }

    return b;
}

void MWDBIndexBin::Deserialize(const std::vector<uint8_t> &data) {
    const uint8_t *p   = data.data();
    const uint8_t *end = p + data.size();

    uint64_t magic = ReadU64(p, end);
    if (magic != MWDB_MAGIC)
        throw std::runtime_error("_index.bin: invalid magic number");

    uint16_t fmt_ver = ReadU16(p, end);
    uint16_t min_ver = ReadU16(p, end);
    (void)min_ver;
    if (fmt_ver > MWDB_FORMAT_VERSION)
        throw std::runtime_error("_index.bin: format version too new");

    next_table_id_ = ReadU64(p, end);
    uint32_t num_entries = ReadU32(p, end);
    root_dir_.clear();
    root_dir_.reserve(num_entries);

    for (uint32_t ei = 0; ei < num_entries; ++ei) {
        MWDBRootEntry e;
        e.type     = (MWDBEntryType)ReadU8(p, end);
        e.table_id = ReadU64(p, end);
        e.name     = ReadStr(p, end);

        if (e.type == MWDBEntryType::TABLE) {
            e.row_count_cached  = ReadU64(p, end);
            e.size_bytes_cached = ReadU64(p, end);

            MWDBTableSection &ts = e.table_section;
            ts.table_id = e.table_id;
            ts.version  = ReadU32(p, end);

            uint32_t num_cols = ReadU32(p, end);
            ts.columns.reserve(num_cols);
            for (uint32_t c = 0; c < num_cols; ++c) {
                MWDBColumnInfo col;
                col.col_id   = ReadU16(p, end);
                col.name     = ReadStr(p, end);
                col.type     = DeserializeType(p, end);
                col.nullable = ReadU8(p, end) != 0;
                col.is_pk    = ReadU8(p, end) != 0;
                ts.columns.push_back(std::move(col));
            }

            uint32_t num_pk = ReadU32(p, end);
            ts.pk_col_ids.resize(num_pk);
            for (auto &id : ts.pk_col_ids) id = ReadU16(p, end);

            uint32_t num_cl = ReadU32(p, end);
            ts.cluster_col_ids.resize(num_cl);
            for (auto &id : ts.cluster_col_ids) id = ReadU16(p, end);

            ts.has_pk_index = ReadU8(p, end) != 0;

            uint32_t num_segs = ReadU32(p, end);
            ts.segments.reserve(num_segs);
            for (uint32_t s = 0; s < num_segs; ++s) {
                MWDBSegmentEntry seg;
                CheckRemain(p, end, 16);
                memcpy(seg.uuid.data(), p, 16); p += 16;
                seg.path       = ReadStr(p, end);
                seg.row_count  = ReadU64(p, end);
                seg.file_size  = ReadU64(p, end);
                seg.written_at = ReadU64(p, end);
                seg.bucket_id  = ReadU32(p, end);
                seg.generation = ReadU16(p, end);
                seg.state      = (MWDBSegmentState)ReadU8(p, end);
                seg.flags      = ReadU32(p, end);
                seg.zone_maps  = DeserializeZoneMaps(p, end);
                ts.segments.push_back(std::move(seg));
            }

            e.section_loaded = true;
        } else if (e.type == MWDBEntryType::VIEW) {
            e.view_sql = ReadStr(p, end);
        }

        root_dir_.push_back(std::move(e));
    }
}

//===--------------------------------------------------------------------===//
// Factory / Load / Save
//===--------------------------------------------------------------------===//

MWDBIndexBin MWDBIndexBin::Create() {
    MWDBIndexBin idx;
    idx.next_table_id_ = 1;
    return idx;
}

MWDBIndexBin MWDBIndexBin::Load(const std::string &db_dir) {
    std::string path = IndexBinPath(db_dir);
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) throw std::runtime_error("Cannot open _index.bin at: " + path);

    auto size = f.tellg();
    f.seekg(0);
    std::vector<uint8_t> data((size_t)size);
    f.read((char*)data.data(), size);
    if (!f) throw std::runtime_error("Failed reading _index.bin at: " + path);

    MWDBIndexBin idx;
    idx.Deserialize(data);
    return idx;
}

void MWDBIndexBin::Save(const std::string &db_dir) const {
    auto data = Serialize();
    std::string final_path = IndexBinPath(db_dir);
    std::string tmp_path   = final_path + ".tmp";

    {
        std::ofstream f(tmp_path, std::ios::binary | std::ios::trunc);
        if (!f) throw std::runtime_error("Cannot write _index.bin.tmp at: " + tmp_path);
        f.write((const char*)data.data(), (std::streamsize)data.size());
        if (!f) throw std::runtime_error("Write failed for _index.bin.tmp");
    } // file is closed (and flushed) here

    // Atomic replace (POSIX rename is atomic on the same filesystem)
    if (::rename(tmp_path.c_str(), final_path.c_str()) != 0)
        throw std::runtime_error("rename() failed for _index.bin");
}

//===--------------------------------------------------------------------===//
// Table management
//===--------------------------------------------------------------------===//

bool MWDBIndexBin::TableExists(const std::string &name) const {
    return FindTable(name) != nullptr;
}

MWDBRootEntry *MWDBIndexBin::FindTable(const std::string &name) {
    for (auto &e : root_dir_)
        if (e.IsTable() && e.name == name) return &e;
    return nullptr;
}

const MWDBRootEntry *MWDBIndexBin::FindTable(const std::string &name) const {
    for (auto &e : root_dir_)
        if (e.IsTable() && e.name == name) return &e;
    return nullptr;
}

uint64_t MWDBIndexBin::AddTable(const std::string &name,
                                 const MWDBTableSection &section) {
    if (TableExists(name))
        throw std::runtime_error("Table already exists: " + name);

    uint64_t tid = next_table_id_++;
    MWDBRootEntry e;
    e.type           = MWDBEntryType::TABLE;
    e.table_id       = tid;
    e.name           = name;
    e.section_loaded = true;
    e.table_section  = section;
    e.table_section.table_id = tid;
    root_dir_.push_back(std::move(e));
    return tid;
}

void MWDBIndexBin::AddView(const std::string &name, const std::string &sql) {
    for (auto &e : root_dir_)
        if (e.name == name)
            throw std::runtime_error("Entry already exists: " + name);
    MWDBRootEntry e;
    e.type     = MWDBEntryType::VIEW;
    e.table_id = next_table_id_++;
    e.name     = name;
    e.view_sql = sql;
    root_dir_.push_back(std::move(e));
}

bool MWDBIndexBin::ViewExists(const std::string &name) const {
    for (auto &e : root_dir_)
        if (e.IsView() && e.name == name) return true;
    return false;
}

void MWDBIndexBin::AddSegment(const std::string &table_name,
                              MWDBSegmentEntry seg) {
    auto *entry = FindTable(table_name);
    if (!entry)
        throw std::runtime_error("Table not found: " + table_name);

    entry->row_count_cached  += seg.row_count;
    entry->size_bytes_cached += seg.file_size;
    entry->table_section.segments.push_back(std::move(seg));
}

void MWDBIndexBin::MarkSegmentObsolete(const std::string &table_name,
                                       const std::array<uint8_t,16> &uuid) {
    auto *entry = FindTable(table_name);
    if (!entry) return;
    for (auto &seg : entry->table_section.segments) {
        if (seg.uuid == uuid) {
            if (seg.state == MWDBSegmentState::LIVE) {
                entry->row_count_cached  -= seg.row_count;
                entry->size_bytes_cached -= seg.file_size;
            }
            seg.state = MWDBSegmentState::OBSOLETE;
            return;
        }
    }
}

} // namespace duckdb
