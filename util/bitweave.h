#pragma once
#include <iostream>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <limits>

#ifdef __AVX2__
#include <immintrin.h>
#endif

// =============================================================
// BitWeaving/H for LevelDB  —  Phase 2 (Option A: per-block tags)
// =============================================================
// One tag per physical SST data block.
// tag[i] summarises the value range of the i-th data block written.
// At query time, BlockReader looks up tag[block_index] before doing
// any I/O.  If MayMatch() returns false the read is skipped entirely.
//
// Serialized layout (columnar, SIMD-friendly):
//   [ 4 bytes: num_blocks (uint32_t) ]
//   [ num_blocks_padded × 4 bytes: all block_min values ]
//   [ num_blocks_padded × 4 bytes: all block_max values ]
//   [ num_blocks_padded × 4 bytes: all bitmasks ]
// num_blocks_padded = ((num_blocks + 7) / 8) * 8  (padding for AVX2 loads)
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
// Serialized format (columnar):
//   [ 4 bytes: num_blocks ]
//   [ num_blocks_padded × 4 bytes: all mins ]
//   [ num_blocks_padded × 4 bytes: all maxes ]
//   [ num_blocks_padded × 4 bytes: all masks ]
// Padding sentinel: min=UINT32_MAX, max=0, mask=0 (never matches)
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
      block_mins_.push_back(std::numeric_limits<uint32_t>::max());
      block_maxs_.push_back(0);
      block_masks_.push_back(0);
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

    block_mins_.push_back(cur_min_);
    block_maxs_.push_back(cur_max_);
    block_masks_.push_back(mask);
    num_blocks_++;
    ResetAccumulator();
  }

  // Call once when the SST is finalised to write the BWH meta-block.
  // Any values accumulated since the last FlushBlock() are flushed
  // automatically (handles the last partial block).
  void Finish(std::string* dst) {
    if (!cur_values_.empty()) {
      FlushBlock();
    }

    if (num_blocks_ == 0) return;

    // Pad arrays to a multiple of 8 so AVX2 loads never read past the end.
    // Sentinel values: min=UINT32_MAX, max=0, mask=0 → never matches any predicate.
    size_t padded = ((num_blocks_ + 7) / 8) * 8;
    while (block_mins_.size() < padded) {
      block_mins_.push_back(std::numeric_limits<uint32_t>::max());
      block_maxs_.push_back(0);
      block_masks_.push_back(0);
    }

    // Columnar layout: [num_blocks][all mins][all maxes][all masks]
    uint32_t nb = static_cast<uint32_t>(num_blocks_);
    dst->append(reinterpret_cast<const char*>(&nb), 4);
    dst->append(reinterpret_cast<const char*>(block_mins_.data()),  padded * 4);
    dst->append(reinterpret_cast<const char*>(block_maxs_.data()),  padded * 4);
    dst->append(reinterpret_cast<const char*>(block_masks_.data()), padded * 4);
  }

  void Reset() {
    block_mins_.clear();
    block_maxs_.clear();
    block_masks_.clear();
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

  // Per-block columnar arrays (one entry per committed block).
  std::vector<uint32_t> block_mins_;
  std::vector<uint32_t> block_maxs_;
  std::vector<uint32_t> block_masks_;
  size_t                num_blocks_ = 0;

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
//
// MayMatchSIMD() evaluates 8 blocks per AVX2 instruction.
// MayMatchBatch() scans all blocks and returns surviving block indices.
// ---------------------------------------------------------------
class BitWeaveReader {
 public:
  BitWeaveReader()
      : data_(nullptr), num_blocks_(0), num_blocks_padded_(0),
        min_data_(nullptr), max_data_(nullptr), mask_data_(nullptr) {}

  // Call once after loading the BWH block from the SST.
  // Returns false if the block is malformed.
  bool Init(const char* data, size_t size) {
    if (size < 4) return false;

    memcpy(&num_blocks_, data, 4);
    num_blocks_padded_ = (num_blocks_ == 0)
                             ? 0
                             : ((num_blocks_ + 7) / 8) * 8;

    size_t expected = 4 + (size_t)num_blocks_padded_ * 12;
    if (size < expected) return false;

    data_      = data;
    min_data_  = reinterpret_cast<const uint32_t*>(data + 4);
    max_data_  = min_data_  + num_blocks_padded_;
    mask_data_ = max_data_  + num_blocks_padded_;
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

    uint32_t bmin = min_data_[block_index];
    uint32_t bmax = max_data_[block_index];

    if (op == '>') return bmax > threshold;
    if (op == '<') return bmin < threshold;
    return true;
  }

  // Returns a bitmask of which blocks in [start_block, start_block+8)
  // MAY contain values matching the predicate.
  // Bit i set → block (start_block+i) must be read.
  // Bit i clear → block can be skipped.
  // Requires AVX2; falls back to scalar when __AVX2__ is not defined.
  // Caller must ensure start_block <= num_blocks_padded_ - 8.
  uint8_t MayMatchSIMD(uint32_t start_block, uint32_t threshold,
                        char op) const {
#ifdef __AVX2__
    // XOR flip converts unsigned → offset-binary so signed _cmpgt works
    // correctly for unsigned values (standard "flip MSB" trick).
    const __m256i flip = _mm256_set1_epi32((int)0x80000000u);
    __m256i thresh_flip =
        _mm256_xor_si256(_mm256_set1_epi32((int)threshold), flip);

    if (op == '>') {
      // Stage 1: ZoneMap — skip blocks where bmax <= threshold.
      __m256i bmax_vec = _mm256_xor_si256(
          _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(max_data_ + start_block)),
          flip);
      __m256i cmp = _mm256_cmpgt_epi32(bmax_vec, thresh_flip);
      uint8_t zonemap_result =
          (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));

      if (zonemap_result == 0) return 0;  // all 8 blocks safely skipped

      // Stage 2: Bitmask refinement — scalar per survivor (band math needs
      // per-lane division which has no AVX2 equivalent).
      uint8_t final_result = 0;
      for (int i = 0; i < 8; i++) {
        if (!((zonemap_result >> i) & 1)) continue;
        uint32_t bmin  = min_data_ [start_block + i];
        uint32_t bmax  = max_data_ [start_block + i];
        uint32_t bmask = mask_data_[start_block + i];

        if (bmax <= bmin) {
          // Degenerate block: all values equal bmin; ZoneMap says bmin > threshold.
          final_result |= (1 << i);
          continue;
        }
        if (threshold < bmin) {
          // All values in block are > threshold.
          final_result |= (1 << i);
          continue;
        }
        // threshold >= bmin: find which band threshold falls in.
        int lo_band = (int)(((uint64_t)(threshold - bmin) * 32)
                            / ((uint64_t)(bmax - bmin + 1)));
        lo_band = std::max(0, std::min(31, lo_band));
        // query_mask selects all bands >= lo_band (values that may exceed threshold).
        uint32_t query_mask = (lo_band == 0) ? 0xFFFFFFFFu
                                             : (0xFFFFFFFFu << lo_band);
        if (bmask & query_mask) final_result |= (1 << i);
      }
      return final_result;
    }

    // op == '<': skip blocks where bmin >= threshold (i.e. threshold <= bmin).
    __m256i bmin_vec = _mm256_xor_si256(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(min_data_ + start_block)),
        flip);
    __m256i cmp = _mm256_cmpgt_epi32(thresh_flip, bmin_vec);
    return (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));

#else  // !__AVX2__ — scalar fallback, one block at a time
    uint8_t result = 0;
    for (int i = 0; i < 8; i++) {
      if (MayMatch(start_block + (uint32_t)i, threshold, op))
        result |= (uint8_t)(1 << i);
    }
    return result;
#endif
  }

  // Scans all blocks and returns the indices of blocks that MAY match.
  // Uses MayMatchSIMD() in steps of 8 (AVX2), falls back to scalar otherwise.

  // ZoneMap-only SIMD — no bitmask stage 2. Pure AVX2 speed measurement.
  uint8_t MayMatchSIMDFast(uint32_t start_block, uint32_t threshold,
                            char op) const {
#ifdef __AVX2__
    const __m256i flip = _mm256_set1_epi32((int)0x80000000u);
    __m256i thresh_flip =
        _mm256_xor_si256(_mm256_set1_epi32((int)threshold), flip);
    if (op == '>') {
      __m256i bmax_vec = _mm256_xor_si256(
          _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(max_data_ + start_block)),
          flip);
      __m256i cmp = _mm256_cmpgt_epi32(bmax_vec, thresh_flip);
      return (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));
    }
    __m256i bmin_vec = _mm256_xor_si256(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(min_data_ + start_block)),
        flip);
    __m256i cmp = _mm256_cmpgt_epi32(thresh_flip, bmin_vec);
    return (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));
#else
    uint8_t r = 0;
    for (int i = 0; i < 8; i++)
      if (MayMatch(start_block+i, threshold, op)) r |= (1<<i);
    return r;
#endif
  }


  // MayMatchBitmaskSIMD — ZoneMap SIMD + bitmask SIMD in two passes.
  // Stage 1: _mm256_cmpgt_epi32 on 8 block_max values (ZoneMap)
  // Stage 2: _mm256_and_si256 on 8 bitmasks vs pre-computed query_mask
  //
  // query_mask is computed ONCE per query by the caller:
  //   uint32_t query_mask = (threshold == 0) ? 0xFFFFFFFFu
  //                       : (0xFFFFFFFFu << approx_band);
  // where approx_band = bw_value_to_band(threshold, global_min, global_max)
  //
  // No per-block division — one AND + one test per 8 blocks.
  // More false positives than per-block local bands, but fully vectorized.
  uint8_t MayMatchBitmaskSIMD(uint32_t start_block,
                               uint32_t threshold,
                               uint32_t query_mask,
                               char op) const {
#ifdef __AVX2__
    const __m256i flip = _mm256_set1_epi32((int)0x80000000u);
    __m256i thresh_flip =
        _mm256_xor_si256(_mm256_set1_epi32((int)threshold), flip);

    // Stage 1: ZoneMap — same as MayMatchSIMDFast
    uint8_t zonemap_bits = 0;
    if (op == '>') {
      __m256i bmax_vec = _mm256_xor_si256(
          _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(max_data_ + start_block)),
          flip);
      __m256i cmp = _mm256_cmpgt_epi32(bmax_vec, thresh_flip);
      zonemap_bits = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));
    } else {
      __m256i bmin_vec = _mm256_xor_si256(
          _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(min_data_ + start_block)),
          flip);
      __m256i cmp = _mm256_cmpgt_epi32(thresh_flip, bmin_vec);
      zonemap_bits = (uint8_t)_mm256_movemask_ps(_mm256_castsi256_ps(cmp));
    }

    // All 8 blocks skipped by ZoneMap — done
    if (zonemap_bits == 0) return 0;

    // Stage 2: Bitmask — AND all 8 bitmasks vs query_mask in one instruction
    __m256i mask_vec   = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(mask_data_ + start_block));
    __m256i qmask_vec  = _mm256_set1_epi32((int)query_mask);
    __m256i anded      = _mm256_and_si256(mask_vec, qmask_vec);

    // Extract which lanes are nonzero — nonzero = block might match
    // Use _mm256_cmpeq_epi32 against zero to find zero lanes
    __m256i zero       = _mm256_setzero_si256();
    __m256i is_zero    = _mm256_cmpeq_epi32(anded, zero);
    uint8_t zero_bits  = (uint8_t)_mm256_movemask_ps(
        _mm256_castsi256_ps(is_zero));
    uint8_t nonzero_bits = (~zero_bits) & 0xFF;  // nonzero = may match

    // Final result: must pass BOTH ZoneMap AND bitmask
    return zonemap_bits & nonzero_bits;
#else
    // scalar fallback
    uint8_t result = 0;
    for (int i = 0; i < 8; i++) {
      uint32_t idx = start_block + (uint32_t)i;
      if (idx >= num_blocks_) break;
      if (!MayMatch(idx, threshold, op)) continue;
      // bitmask check
      if (mask_data_[idx] & query_mask) result |= (1 << i);
    }
    return result;
#endif
  }

  // Helper: compute the global query_mask for a threshold.
  // Uses the min/max of the first and last block as an approximation
  // of the dataset range. Call once per query, pass result to
  // MayMatchBitmaskSIMD().
  uint32_t ComputeQueryMask(uint32_t threshold, char op) const {
    if (num_blocks_ == 0) return 0xFFFFFFFFu;
    // approximate global range from first + last block
    uint32_t global_min = min_data_[0];
    uint32_t global_max = max_data_[0];
    for (uint32_t i = 1; i < num_blocks_ && i < 32; i++) {
      if (min_data_[i] < global_min) global_min = min_data_[i];
      if (max_data_[i] > global_max) global_max = max_data_[i];
    }
    if (global_max <= global_min) return 0xFFFFFFFFu;
    if (op == '>') {
      int lo_band = bw_value_to_band(threshold, global_min, global_max);
      if (lo_band <= 0) return 0xFFFFFFFFu;
      return 0xFFFFFFFFu << lo_band;
    } else {
      int hi_band = bw_value_to_band(threshold, global_min, global_max);
      if (hi_band >= 31) return 0xFFFFFFFFu;
      return (1u << (hi_band + 1)) - 1u;
    }
  }

  std::vector<uint32_t> MayMatchBatch(uint32_t threshold, char op) const {
    std::vector<uint32_t> result;
    if (!valid()) return result;

#ifdef __AVX2__
    for (uint32_t start = 0; start < num_blocks_padded_; start += 8) {
      uint8_t bits = MayMatchSIMD(start, threshold, op);
      if (bits == 0) continue;
      for (int i = 0; i < 8; i++) {
        uint32_t idx = start + (uint32_t)i;
        if (idx >= num_blocks_) break;          // tail padding
        if ((bits >> i) & 1) result.push_back(idx);
      }
    }
#else
    for (uint32_t b = 0; b < num_blocks_; b++) {
      if (MayMatch(b, threshold, op)) result.push_back(b);
    }
#endif
    return result;
  }

  bool     valid()              const { return data_ != nullptr; }
  uint32_t num_blocks()         const { return num_blocks_; }
  uint32_t num_blocks_padded()  const { return num_blocks_padded_; }

 private:
  const char*     data_;              // base pointer into loaded BWH block
  uint32_t        num_blocks_;        // actual number of blocks
  uint32_t        num_blocks_padded_; // padded to multiple of 8

  // Three columnar pointers into the BWH block (set by Init()).
  const uint32_t* min_data_;
  const uint32_t* max_data_;
  const uint32_t* mask_data_;
};

} // namespace leveldb
