//===----------------------------------------------------------------------===//
// mwdb_pk_index.hpp
// Optional global primary-key index for the multi-writer DuckDB storage.
//
// Maps primary-key byte sequences → (segment_index, row_position).
// Stored as a sorted flat array in _index.bin so that lookup is O(log N)
// binary search — behaviour identical to an ART for point queries.
//
// ZERO OVERHEAD when absent:
//   if (!pk_index.HasIndex()) return; // single flag check, no further work
//
// Thread safety: the index is rebuilt/loaded once and then immutable until
// the next segment flush updates _index.bin (at which point the caller
// reloads from disk under whatever locking policy is appropriate).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <utility>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Lightweight optional (C++11 compatible, no Boost dependency)
//===--------------------------------------------------------------------===//

template <typename T>
struct MWDBOptional {
	bool has_value = false;
	T    value {};

	MWDBOptional() : has_value(false) {}
	explicit MWDBOptional(T v) : has_value(true), value(std::move(v)) {}

	explicit operator bool() const { return has_value; }
};

//===--------------------------------------------------------------------===//
// Single entry in the sorted index
//===--------------------------------------------------------------------===//

struct MWDBPKEntry {
    std::vector<uint8_t> key_bytes;   // comparable byte representation of PK
    uint32_t             segment_idx; // index into MWDBTableSection::segments
    uint32_t             row_pos;     // 0-based row offset within that segment

    bool operator<(const MWDBPKEntry &o) const {
        return key_bytes < o.key_bytes;
    }
    bool operator==(const MWDBPKEntry &o) const {
        return key_bytes == o.key_bytes;
    }
};

//===--------------------------------------------------------------------===//
// Key serialization helpers (radix-order preserving, like DuckDB ARTKey)
//===--------------------------------------------------------------------===//

class MWDBKeyEncoder {
public:
    // Encode a 64-bit integer in big-endian with sign flip so that the
    // natural byte order matches numeric order (i.e., -1 < 0 < 1).
    static std::vector<uint8_t> EncodeInt64(int64_t v);
    static std::vector<uint8_t> EncodeUInt64(uint64_t v);
    static std::vector<uint8_t> EncodeInt32(int32_t v);
    static std::vector<uint8_t> EncodeUInt32(uint32_t v);
    static std::vector<uint8_t> EncodeDouble(double v);
    // Strings: UTF-8 bytes followed by a 0x00 sentinel so that prefix
    // ordering is preserved and "abc" < "abcd".
    static std::vector<uint8_t> EncodeString(const std::string &s);
    // Composite key: concatenate individual encodings.
    static void Append(std::vector<uint8_t> &dst, const std::vector<uint8_t> &src);
};

//===--------------------------------------------------------------------===//
// The index itself
//===--------------------------------------------------------------------===//

class MWDBPKIndex {
public:
    MWDBPKIndex() : has_index_(false) {}

    //------------------------------------------------------------------
    // Lifecycle
    //------------------------------------------------------------------

    // Build a brand-new empty index (call Insert() then Finalize()).
    static MWDBPKIndex CreateEmpty();

    // Serialise to bytes for storage inside _index.bin.
    std::vector<uint8_t> Serialize() const;

    // Deserialise from bytes previously written by Serialize().
    static MWDBPKIndex Deserialize(const uint8_t *data, size_t len);

    //------------------------------------------------------------------
    // Zero-overhead absent check
    // When HasIndex() == false every lookup returns an empty optional and
    // every Insert() is a no-op. No iteration, no allocation.
    //------------------------------------------------------------------
    bool HasIndex() const { return has_index_; }
    void Enable()         { has_index_ = true;  }

    //------------------------------------------------------------------
    // Build phase (call before Finalize)
    //------------------------------------------------------------------
    void Insert(std::vector<uint8_t> key, uint32_t seg_idx, uint32_t row_pos);

    // Sort the entries so that Lookup() works. Must be called after all
    // Insert() calls and before any Lookup().
    void Finalize();

    // Merge another index (e.g. newly written segment) into this one and
    // re-sort. Keeps the index up to date without a full rebuild.
    void Merge(MWDBPKIndex &&other);

    // Remove all entries belonging to a segment (called when a segment
    // becomes obsolete after compaction).
    void RemoveSegment(uint32_t seg_idx);

    //------------------------------------------------------------------
    // Lookup
    //------------------------------------------------------------------

    // Returns (segment_idx, row_pos) if found, else an empty optional.
    // O(log N) binary search — O(1) when HasIndex() == false.
    MWDBOptional<std::pair<uint32_t,uint32_t>>
    Lookup(const std::vector<uint8_t> &key) const;

    // Range scan: all entries with key in [lo, hi] (inclusive).
    // Empty result when HasIndex() == false.
    std::vector<const MWDBPKEntry*>
    Range(const std::vector<uint8_t> &lo, const std::vector<uint8_t> &hi) const;

    //------------------------------------------------------------------
    // Stats
    //------------------------------------------------------------------
    size_t EntryCount() const { return entries_.size(); }

private:
    bool                      has_index_;
    std::vector<MWDBPKEntry>  entries_;  // sorted after Finalize()
    bool                      sorted_ = false;
};

} // namespace duckdb
