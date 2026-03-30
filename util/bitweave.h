#pragma once
#include <iostream>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <limits>

// =============================================================
// BitWeaving/H for LevelDB  —  Phase 2 (Option A: per-block tags)
// =============================================================
// One tag per physical SST data block.
// tag[i] summarises the value range of the i-th data block written.
// At query time, BlockReader looks up tag[block_index] before doing
// any I/O.  If MayMatch() returns false the read is skipped entirely.
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

// The name used to store our block in LevelDB's Meta-Index.
static const char* BW_BLOCK_NAME = "bitweave.leveldb.BWH";


// ---------------------------------------------------------------
// Helper: map a single value into a band number [0..31]
// ---------------------------------------------------------------
inline int bw_value_to_band(uint32_t value, uint32_t min_val, uint32_t max_val) {
  if (max_val <= min_val) return 0;
  uint64_t range  = (uint64_t)max_val - min_val + 1;
  uint64_t offset = (value < min_val) ? 0 : (uint64_t)value - min_val;
  int band = (int)((offset * BW_NUM_BANDS) / range);
  if (band < 0)             band = 0;
  if (band >= BW_NUM_BANDS) band = BW_NUM_BANDS - 1;
  return band;
}


// ---------------------------------------------------------------
// BitWeaveBuilder  (Option A: one tag per physical data block)
//
// Usage in table_builder.cc:
//   1. Call AddValue(v) for every record added to the current block.
//   2. Call FlushBlock() when WriteBlock() is called for a data block.
//      This commits the current block's accumulated min/max/mask as
//      one tag entry and resets the accumulator for the next block.
//   3. Call Finish(dst) once, at SST finalisation, to serialise all
//      committed tags into the BWH meta-block.
//
// Block format written:
//   [ 4 bytes: num_blocks (uint32_t) ]
//   Per block:
//     [ 4 bytes: block_min  (uint32_t) ]
//     [ 4 bytes: block_max  (uint32_t) ]
//     [ 4 bytes: bitmask    (uint32_t) ]
//   Total per block: 12 bytes
// ---------------------------------------------------------------
class BitWeaveBuilder {
 public:
  BitWeaveBuilder() { ResetAccumulator(); }

  // Call once per record value as the SST is being built.
  void AddValue(uint32_t value) {
    if (value < cur_min_) cur_min_ = value;
    if (value > cur_max_) cur_max_ = value;
    cur_values_.push_back(value);
  }

  // Call at every WriteBlock() for a data block.
  // Commits the current accumulator as one tag and resets it.
  void FlushBlock() {
    if (cur_values_.empty()) {
      // Empty block: emit a tag that never matches anything.
      uint32_t entry[3] = {
          std::numeric_limits<uint32_t>::max(),  // min
          0,                                      // max  (max < min → impossible range)
          0                                       // mask (all bands empty)
      };
      tags_.append(reinterpret_cast<char*>(entry), 12);
      num_blocks_++;
      ResetAccumulator();
      return;
    }

    // Build bitmask using this block's own local min/max
    // (local normalisation gives better band precision per block).
    uint32_t mask = 0;
    for (uint32_t v : cur_values_) {
      int band = bw_value_to_band(v, cur_min_, cur_max_);
      mask |= (1u << band);
    }

    uint32_t entry[3] = { cur_min_, cur_max_, mask };
    tags_.append(reinterpret_cast<char*>(entry), 12);
    num_blocks_++;
    ResetAccumulator();
  }

  // Call once when the SST is finalised to write the BWH meta-block.
  // Any values accumulated since the last FlushBlock() are flushed
  // automatically (handles the last partial block).
  void Finish(std::string* dst) {
    // Flush any trailing values that didn't get a FlushBlock() call
    // (this happens for the very last data block if Finish() is called
    //  before the caller has a chance to call FlushBlock()).
    if (!cur_values_.empty()) {
      FlushBlock();
    }

    if (num_blocks_ == 0) return;

    // Write: [num_blocks][tag0][tag1]...[tagN-1]
    uint32_t nb = static_cast<uint32_t>(num_blocks_);
    dst->append(reinterpret_cast<char*>(&nb), 4);
    dst->append(tags_);
  }

  void Reset() {
    tags_.clear();
    num_blocks_ = 0;
    ResetAccumulator();
  }

  size_t NumBlocks() const { return num_blocks_; }
  // Returns true if there are uncommitted values (i.e. AddValue() was
  // called since the last FlushBlock()).
  bool HasPendingValues() const { return !cur_values_.empty(); }

 private:
  void ResetAccumulator() {
    cur_min_ = std::numeric_limits<uint32_t>::max();
    cur_max_ = 0;
    cur_values_.clear();
  }

  // Committed tag bytes (each group is 12 bytes: min, max, mask).
  std::string tags_;
  size_t      num_blocks_ = 0;

  // Accumulator for the block currently being built.
  uint32_t              cur_min_;
  uint32_t              cur_max_;
  std::vector<uint32_t> cur_values_;
};


// ---------------------------------------------------------------
// BitWeaveReader
//
// Used inside Table when READING an SST file.
// Call Init() with the raw BWH block bytes once when SST is opened.
// Call MayMatch() for each data block during a scan.
//
// MayMatch returns:
//   true  → block MIGHT contain matching values → proceed with I/O
//   false → block CANNOT contain matching values → skip I/O
//
// false negatives are impossible (we never wrongly skip).
// false positives are ok  (we sometimes read when not needed).
// ---------------------------------------------------------------
class BitWeaveReader {
 public:
  BitWeaveReader() : data_(nullptr), num_blocks_(0) {}

  // Call once after loading the BWH block from the SST.
  // Returns false if the block is malformed.
  bool Init(const char* data, size_t size) {
    if (size < 4) return false;

    memcpy(&num_blocks_, data, 4);

    size_t expected = 4 + (size_t)num_blocks_ * 12;
    if (size < expected) return false;

    // Point past the 4-byte header to the first tag entry.
    data_ = data + 4;
    return true;
  }

  // Call before issuing I/O for the block with the given index.
  // block_index is 0-based; it must match the order tags were emitted
  // by BitWeaveBuilder::FlushBlock().
  //
  // threshold: the literal value in the WHERE clause  (e.g. 800)
  // op:        '>' or '<'
 bool MayMatch(uint32_t block_index, uint32_t threshold, char op) const {
if (data_ == nullptr || block_index >= num_blocks_) return true;

    const uint32_t* entry =
        reinterpret_cast<const uint32_t*>(data_ + block_index * 12);
    uint32_t bmin = entry[0];
    uint32_t bmax = entry[1];
    uint32_t mask = entry[2];

   

    // Simple min/max range check
    if (op == '>') {
      return bmax > threshold;
    } else if (op == '<') {
      return bmin < threshold;
    }
    
    return true;
}

  bool     valid()       const { return data_ != nullptr; }
  uint32_t num_blocks()  const { return num_blocks_; }

 private:
  const char* data_;        // points into the loaded BWH block
  uint32_t    num_blocks_;
};

} // namespace leveldb