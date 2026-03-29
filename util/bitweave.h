#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

// =============================================================
// BitWeaving/H for LevelDB
// =============================================================
// One idea: instead of reading every record to check a predicate,
// store a tiny bitmask per group of records that summarizes
// which "value bands" are present in that group.
// At query time, check the bitmask — if the queried band
// is not set, skip the entire group.
//
// Three things in this file:
//   1. Constants
//   2. BitWeaveBuilder  — used when WRITING an SST
//   3. BitWeaveReader   — used when READING an SST
// =============================================================

namespace leveldb {

// ---------------------------------------------------------------
// Constants
// ---------------------------------------------------------------

// How many bands to divide the value range into.
// 32 fits in one uint32_t — one bit per band.
static const int BW_NUM_BANDS  = 32;

// How many records per group.
// 64 is one cache line worth of bitmask checks.
static const int BW_GROUP_SIZE = 64;

// The name used to store our block in LevelDB's Meta-Index.
// LevelDB looks up blocks by name — this is how it finds ours.
static const char* BW_BLOCK_NAME = "bitweave.leveldb.BWH";


// ---------------------------------------------------------------
// Helper: map a single value into a band number [0..31]
//
// Example:
//   value=750, min=0, max=1000
//   offset = 750 - 0 = 750
//   band   = (750 * 32) / 1001 = 23
// ---------------------------------------------------------------
inline int bw_value_to_band(uint32_t value, uint32_t min_val, uint32_t max_val) {
  if (max_val <= min_val) return 0;
  uint64_t range  = (uint64_t)max_val - min_val + 1;
  uint64_t offset = (value < min_val) ? 0 : (uint64_t)value - min_val;
  int band = (int)((offset * BW_NUM_BANDS) / range);
  if (band < 0)            band = 0;
  if (band >= BW_NUM_BANDS) band = BW_NUM_BANDS - 1;
  return band;
}


// ---------------------------------------------------------------
// BitWeaveBuilder
//
// Used inside TableBuilder when WRITING an SST file.
// Call AddValue() for every record as it's added.
// Call Finish() when the SST is done — it returns the block bytes.
//
// The block format we write:
//
//   [ 4 bytes: num_groups ]
//   Per group:
//     [ 4 bytes: group_min ]
//     [ 4 bytes: group_max ]
//     [ 4 bytes: bitmask   ]
//   Total per group: 12 bytes
// ---------------------------------------------------------------
class BitWeaveBuilder {
 public:
  BitWeaveBuilder() {}

  // Call once per record as the SST is being built.
  void AddValue(uint32_t value) {
    pending_.push_back(value);
  }

  // Call once when the SST is finalized.
  // Appends the complete BitWeaving block to dst.
  void Finish(std::string* dst) {
    if (pending_.empty()) return;

    size_t num_groups = (pending_.size() + BW_GROUP_SIZE - 1) / BW_GROUP_SIZE;

    // Write num_groups as first 4 bytes
    uint32_t ng = (uint32_t)num_groups;
    dst->append((char*)&ng, 4);

    // Write one entry per group: [min, max, bitmask]
    for (size_t g = 0; g < num_groups; g++) {
      size_t start = g * BW_GROUP_SIZE;
      size_t end   = std::min(start + BW_GROUP_SIZE, pending_.size());

      // Find min and max for this group
      uint32_t gmin = pending_[start];
      uint32_t gmax = pending_[start];
      for (size_t i = start + 1; i < end; i++) {
        if (pending_[i] < gmin) gmin = pending_[i];
        if (pending_[i] > gmax) gmax = pending_[i];
      }

      // Build the bitmask using this group's own min/max
      // (local range = precise bands = better skip ratio)
      uint32_t mask = 0;
      for (size_t i = start; i < end; i++) {
        int band = bw_value_to_band(pending_[i], gmin, gmax);
        mask |= (1u << band);
      }

      // Write [gmin, gmax, mask] — 12 bytes
      uint32_t entry[3] = { gmin, gmax, mask };
      dst->append((char*)entry, 12);
    }
  }

  void Reset() { pending_.clear(); }
  size_t NumValues() const { return pending_.size(); }

 private:
  std::vector<uint32_t> pending_;
};


// ---------------------------------------------------------------
// BitWeaveReader
//
// Used inside Table when READING an SST file.
// Call Init() with the raw block bytes once when SST is opened.
// Call MayMatch() for each group during a scan to decide skip/read.
//
// MayMatch returns:
//   true  → group MIGHT have matching values → READ it
//   false → group CANNOT have matching values → SKIP it
//
// false negatives are impossible (we never wrongly skip).
// false positives are ok (we sometimes read when we don't need to).
// ---------------------------------------------------------------
class BitWeaveReader {
 public:
  BitWeaveReader() : data_(nullptr), num_groups_(0) {}

  // Call once after loading the block from SST.
  // Returns false if the block is malformed.
  bool Init(const char* data, size_t size) {
    if (size < 4) return false;

    // Read num_groups from first 4 bytes
    memcpy(&num_groups_, data, 4);

    // Verify we have enough data
    size_t expected = 4 + num_groups_ * 12;
    if (size < expected) return false;

    // Point to the group entries (right after the 4-byte header)
    data_ = data + 4;
    return true;
  }

  // Call before reading each data block during a scan.
  // group_index: 0-based index of this group
  // threshold:   the value in the WHERE clause  (e.g. 800)
  // op:          '>' or '<'
  bool MayMatch(uint32_t group_index, uint32_t threshold, char op) const {
    if (data_ == nullptr || group_index >= num_groups_) return true;

    // Read this group's entry: [gmin, gmax, mask]
    const uint32_t* entry = (const uint32_t*)(data_ + group_index * 12);
    uint32_t gmin = entry[0];
    uint32_t gmax = entry[1];
    uint32_t mask = entry[2];

    // Fast path: check range before touching bitmask
    if (op == '>' && threshold >= gmax) return false; // all values <= gmax <= threshold
    if (op == '<' && threshold <= gmin) return false; // all values >= gmin >= threshold

    // Which bands are "relevant" for this predicate?
    uint32_t relevant = 0;
    if (op == '>') {
      // All bands at or above the threshold band
      int tband = bw_value_to_band(threshold, gmin, gmax);
      for (int b = tband; b < BW_NUM_BANDS; b++)
        relevant |= (1u << b);
    } else if (op == '<') {
      // All bands at or below the threshold band
      int tband = bw_value_to_band(threshold, gmin, gmax);
      for (int b = 0; b <= tband; b++)
        relevant |= (1u << b);
    }

    // If no relevant bands are set → skip
    return (mask & relevant) != 0;
  }

  bool valid()      const { return data_ != nullptr; }
  uint32_t groups() const { return num_groups_; }

 private:
  const char* data_;        // points into the loaded block
  uint32_t    num_groups_;
};

} // namespace leveldb