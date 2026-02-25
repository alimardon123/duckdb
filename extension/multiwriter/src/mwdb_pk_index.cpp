//===----------------------------------------------------------------------===//
// mwdb_pk_index.cpp  –  Optional global PK index implementation
//===----------------------------------------------------------------------===//
#include "mwdb_pk_index.hpp"

#include <algorithm>
#include <cstring>
#include <cstdint>
#include <stdexcept>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MWDBKeyEncoder helpers
//===--------------------------------------------------------------------===//

std::vector<uint8_t> MWDBKeyEncoder::EncodeUInt64(uint64_t v) {
    std::vector<uint8_t> out(8);
    out[0] = (v >> 56) & 0xFF;
    out[1] = (v >> 48) & 0xFF;
    out[2] = (v >> 40) & 0xFF;
    out[3] = (v >> 32) & 0xFF;
    out[4] = (v >> 24) & 0xFF;
    out[5] = (v >> 16) & 0xFF;
    out[6] = (v >>  8) & 0xFF;
    out[7] = (v      ) & 0xFF;
    return out;
}

std::vector<uint8_t> MWDBKeyEncoder::EncodeInt64(int64_t v) {
    // Flip sign bit so that -MAX < ... < -1 < 0 < 1 < ... < MAX in byte order.
    uint64_t u = (uint64_t)v ^ (uint64_t(1) << 63);
    return EncodeUInt64(u);
}

std::vector<uint8_t> MWDBKeyEncoder::EncodeUInt32(uint32_t v) {
    std::vector<uint8_t> out(4);
    out[0] = (v >> 24) & 0xFF;
    out[1] = (v >> 16) & 0xFF;
    out[2] = (v >>  8) & 0xFF;
    out[3] = (v      ) & 0xFF;
    return out;
}

std::vector<uint8_t> MWDBKeyEncoder::EncodeInt32(int32_t v) {
    uint32_t u = (uint32_t)v ^ (uint32_t(1) << 31);
    return EncodeUInt32(u);
}

std::vector<uint8_t> MWDBKeyEncoder::EncodeDouble(double v) {
    uint64_t u;
    memcpy(&u, &v, 8);
    // For positive: flip sign bit only.
    // For negative: flip all bits (makes negatives sort correctly).
    if (u >> 63) {
        u = ~u;
    } else {
        u ^= (uint64_t(1) << 63);
    }
    return EncodeUInt64(u);
}

std::vector<uint8_t> MWDBKeyEncoder::EncodeString(const std::string &s) {
    std::vector<uint8_t> out(s.size() + 1);
    memcpy(out.data(), s.data(), s.size());
    out[s.size()] = 0x00; // sentinel
    return out;
}

void MWDBKeyEncoder::Append(std::vector<uint8_t> &dst, const std::vector<uint8_t> &src) {
    dst.insert(dst.end(), src.begin(), src.end());
}

//===--------------------------------------------------------------------===//
// MWDBPKIndex
//===--------------------------------------------------------------------===//

MWDBPKIndex MWDBPKIndex::CreateEmpty() {
    MWDBPKIndex idx;
    idx.has_index_ = true;
    idx.sorted_    = false;
    return idx;
}

void MWDBPKIndex::Insert(std::vector<uint8_t> key, uint32_t seg_idx, uint32_t row_pos) {
    if (!has_index_) return;
    MWDBPKEntry e;
    e.key_bytes  = std::move(key);
    e.segment_idx = seg_idx;
    e.row_pos    = row_pos;
    entries_.push_back(std::move(e));
    sorted_ = false;
}

void MWDBPKIndex::Finalize() {
    if (!has_index_) return;
    std::sort(entries_.begin(), entries_.end());
    sorted_ = true;
}

void MWDBPKIndex::Merge(MWDBPKIndex &&other) {
    if (!has_index_) return;
    entries_.insert(entries_.end(),
                    std::make_move_iterator(other.entries_.begin()),
                    std::make_move_iterator(other.entries_.end()));
    Finalize();
}

void MWDBPKIndex::RemoveSegment(uint32_t seg_idx) {
    if (!has_index_) return;
    entries_.erase(
        std::remove_if(entries_.begin(), entries_.end(),
            [seg_idx](const MWDBPKEntry &e){ return e.segment_idx == seg_idx; }),
        entries_.end());
    // Still sorted after removal (we only removed elements, not reordered).
}

std::optional<std::pair<uint32_t,uint32_t>>
MWDBPKIndex::Lookup(const std::vector<uint8_t> &key) const {
    if (!has_index_ || entries_.empty()) return std::nullopt;

    // Binary search: create a dummy entry to compare
    MWDBPKEntry target;
    target.key_bytes = key;
    auto it = std::lower_bound(entries_.begin(), entries_.end(), target);
    if (it != entries_.end() && it->key_bytes == key) {
        return std::make_pair(it->segment_idx, it->row_pos);
    }
    return std::nullopt;
}

std::vector<const MWDBPKEntry*>
MWDBPKIndex::Range(const std::vector<uint8_t> &lo, const std::vector<uint8_t> &hi) const {
    if (!has_index_) return {};

    MWDBPKEntry lo_entry, hi_entry;
    lo_entry.key_bytes = lo;
    hi_entry.key_bytes = hi;

    auto first = std::lower_bound(entries_.begin(), entries_.end(), lo_entry);
    // upper_bound for hi (inclusive): find first element > hi
    auto last  = std::upper_bound(entries_.begin(), entries_.end(), hi_entry);

    std::vector<const MWDBPKEntry*> result;
    result.reserve((size_t)(last - first));
    for (auto it = first; it != last; ++it) {
        result.push_back(&*it);
    }
    return result;
}

//===--------------------------------------------------------------------===//
// Serialisation helpers (inline)
//===--------------------------------------------------------------------===//

static void WriteU32(std::vector<uint8_t> &b, uint32_t v) {
    b.push_back( v        & 0xFF);
    b.push_back((v >>  8) & 0xFF);
    b.push_back((v >> 16) & 0xFF);
    b.push_back((v >> 24) & 0xFF);
}
static void WriteU64(std::vector<uint8_t> &b, uint64_t v) {
    for (int i = 0; i < 8; ++i) b.push_back((v >> (8*i)) & 0xFF);
}

static void CheckBuf(const uint8_t *p, const uint8_t *end, size_t need) {
    if ((size_t)(end - p) < need)
        throw std::runtime_error("PK index: unexpected end of data");
}
static uint32_t ReadU32(const uint8_t *&p, const uint8_t *end) {
    CheckBuf(p, end, 4);
    uint32_t v = (uint32_t)p[0] | ((uint32_t)p[1]<<8) |
                 ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
    p += 4; return v;
}
static uint64_t ReadU64(const uint8_t *&p, const uint8_t *end) {
    CheckBuf(p, end, 8);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= ((uint64_t)p[i] << (8*i));
    p += 8; return v;
}

std::vector<uint8_t> MWDBPKIndex::Serialize() const {
    std::vector<uint8_t> buf;
    buf.push_back(has_index_ ? 1 : 0);
    if (!has_index_) return buf;

    WriteU64(buf, (uint64_t)entries_.size());
    for (auto &e : entries_) {
        WriteU32(buf, (uint32_t)e.key_bytes.size());
        buf.insert(buf.end(), e.key_bytes.begin(), e.key_bytes.end());
        WriteU32(buf, e.segment_idx);
        WriteU32(buf, e.row_pos);
    }
    return buf;
}

MWDBPKIndex MWDBPKIndex::Deserialize(const uint8_t *data, size_t len) {
    MWDBPKIndex idx;
    const uint8_t *p   = data;
    const uint8_t *end = data + len;

    CheckBuf(p, end, 1);
    idx.has_index_ = (*p++ != 0);
    if (!idx.has_index_) return idx;

    uint64_t n = ReadU64(p, end);
    idx.entries_.reserve((size_t)n);
    for (uint64_t i = 0; i < n; ++i) {
        MWDBPKEntry e;
        uint32_t key_len = ReadU32(p, end);
        CheckBuf(p, end, key_len);
        e.key_bytes.assign(p, p + key_len); p += key_len;
        e.segment_idx = ReadU32(p, end);
        e.row_pos     = ReadU32(p, end);
        idx.entries_.push_back(std::move(e));
    }
    idx.sorted_ = true; // assume serialized in sorted order
    return idx;
}

} // namespace duckdb
