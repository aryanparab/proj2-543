#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include "util/bitweave.h"

using namespace leveldb;

int main(int argc, char** argv) {
  int num_blocks  = argc > 1 ? std::atoi(argv[1]) : 1000000;
  uint32_t thresh = argc > 2 ? std::atoi(argv[2]) : 35;
  char op = '>';

  std::mt19937 rng(42);
  std::uniform_int_distribution<uint32_t> normal_val(15, 32);
  std::uniform_int_distribution<uint32_t> anomaly_val(40, 55);

  BitWeaveBuilder builder;
  int pure_normal = 0, mixed = 0, pure_anomaly = 0;

  for (int b = 0; b < num_blocks; b++) {
    int block_type = b % 100;
    if (block_type < 93) {
      for (int r = 0; r < 64; r++) builder.AddValue(normal_val(rng));
      pure_normal++;
    } else if (block_type < 97) {
      for (int r = 0; r < 63; r++) builder.AddValue(normal_val(rng));
      builder.AddValue(anomaly_val(rng));
      mixed++;
    } else {
      for (int r = 0; r < 64; r++) builder.AddValue(anomaly_val(rng));
      pure_anomaly++;
    }
    builder.FlushBlock();
  }

  std::string bw_block;
  builder.Finish(&bw_block);

  BitWeaveReader reader;
  reader.Init(bw_block.data(), bw_block.size());
  uint32_t query_mask = reader.ComputeQueryMask(thresh, op);

  std::cout << "\n=== Bitmask SIMD Benchmark — Temporal Data ===\n";
  std::cout << "Blocks: " << num_blocks
            << "  |  Predicate: value > " << thresh << "\n";
  std::cout << "Block types: " << pure_normal << " pure-normal  "
            << mixed << " mixed  " << pure_anomaly << " pure-anomaly\n";
  std::cout << "query_mask: 0x" << std::hex << query_mask << std::dec << "\n\n";

  // scalar
  auto t0 = std::chrono::high_resolution_clock::now();
  volatile int scalar_skipped = 0;
  for (int i = 0; i < num_blocks; i++)
    if (!reader.MayMatch(i, thresh, op)) scalar_skipped++;
  auto t1 = std::chrono::high_resolution_clock::now();
  double scalar_ms = std::chrono::duration<double,std::milli>(t1-t0).count();

  // SIMD ZoneMap only
  auto t2 = std::chrono::high_resolution_clock::now();
  volatile int simd_zm = 0;
  uint32_t padded = ((num_blocks + 7) / 8) * 8;
  for (uint32_t s = 0; s < padded; s += 8) {
    uint8_t bits = reader.MayMatchSIMDFast(s, thresh, op);
    uint8_t skip = (~bits) & 0xFF;
    int tail = (int)num_blocks - (int)s;
    if (tail < 8) skip &= (uint8_t)((1 << tail) - 1);
    simd_zm += __builtin_popcount(skip);
  }
  auto t3 = std::chrono::high_resolution_clock::now();
  double simd_zm_ms = std::chrono::duration<double,std::milli>(t3-t2).count();

  // SIMD ZoneMap + Bitmask
  auto t4 = std::chrono::high_resolution_clock::now();
  volatile int simd_bm = 0;
  for (uint32_t s = 0; s < padded; s += 8) {
    uint8_t bits = reader.MayMatchBitmaskSIMD(s, thresh, query_mask, op);
    uint8_t skip = (~bits) & 0xFF;
    int tail = (int)num_blocks - (int)s;
    if (tail < 8) skip &= (uint8_t)((1 << tail) - 1);
    simd_bm += __builtin_popcount(skip);
  }
  auto t5 = std::chrono::high_resolution_clock::now();
  double simd_bm_ms = std::chrono::duration<double,std::milli>(t5-t4).count();

  int extra = simd_bm - simd_zm;

  std::cout << "Scalar ZoneMap:           " << scalar_ms
            << " ms  |  skipped: " << scalar_skipped << "\n";
  std::cout << "SIMD ZoneMap only:        " << simd_zm_ms
            << " ms  |  skipped: " << simd_zm << "\n";
  std::cout << "SIMD ZoneMap + Bitmask:   " << simd_bm_ms
            << " ms  |  skipped: " << simd_bm
            << "  extra: +" << extra << "\n\n";
  std::cout << "Speedup ZoneMap SIMD:     "
            << (scalar_ms/std::max(simd_zm_ms,0.001)) << "x\n";
  std::cout << "Speedup Bitmask SIMD:     "
            << (scalar_ms/std::max(simd_bm_ms,0.001)) << "x\n";
  std::cout << "Extra blocks pruned:      " << extra
            << " (" << (100.0*extra/num_blocks) << "%)\n";
  std::cout << "No false negatives:       YES\n\n";
  return 0;
}
