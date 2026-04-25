#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include "util/bitweave.h"

using namespace leveldb;

int main(int argc, char** argv) {
  int num_blocks  = argc > 1 ? std::atoi(argv[1]) : 1000000;
  uint32_t thresh = argc > 2 ? std::atoi(argv[2]) : 990;
  char op = '>';

  std::mt19937 rng(42);
  std::uniform_int_distribution<uint32_t> val_dist(0, 1000);

  BitWeaveBuilder builder;
  for (int b = 0; b < num_blocks; b++) {
    for (int r = 0; r < 100; r++)
      builder.AddValue(val_dist(rng));
    builder.FlushBlock();
  }
  std::string bw_block;
  builder.Finish(&bw_block);

  BitWeaveReader reader;
  reader.Init(bw_block.data(), bw_block.size());

  std::cout << "\n=== SIMD BitWeaving Benchmark ===\n";
  std::cout << "Blocks: " << num_blocks
            << "  |  Predicate: value > " << thresh << "\n\n";

  // --- scalar path ---
  auto t0 = std::chrono::high_resolution_clock::now();
  volatile int scalar_skipped = 0;
  for (int i = 0; i < num_blocks; i++)
    if (!reader.MayMatch(i, thresh, op)) scalar_skipped++;
  auto t1 = std::chrono::high_resolution_clock::now();
  double scalar_ms = std::chrono::duration<double,std::milli>(t1-t0).count();

  // --- SIMD path: pure popcount, no inner loop ---
  auto t2 = std::chrono::high_resolution_clock::now();
  volatile int simd_skipped = 0;
  uint32_t padded = ((num_blocks + 7) / 8) * 8;
  for (uint32_t start = 0; start < padded; start += 8) {
    uint8_t bits     = reader.MayMatchSIMDFast(start, thresh, op);
    uint8_t skip_bits = (~bits) & 0xFF;
    // handle tail: zero out bits beyond num_blocks
    int tail = (int)num_blocks - (int)start;
    if (tail < 8) skip_bits &= (uint8_t)((1 << tail) - 1);
    simd_skipped += __builtin_popcount(skip_bits);
  }
  auto t3 = std::chrono::high_resolution_clock::now();
  double simd_ms = std::chrono::duration<double,std::milli>(t3-t2).count();

  bool match = (scalar_skipped == simd_skipped);

  std::cout << "Scalar path:  " << scalar_ms << " ms"
            << "  |  blocks skipped: " << scalar_skipped << "\n";
  std::cout << "SIMD path:    " << simd_ms   << " ms"
            << "  |  blocks skipped: " << simd_skipped   << "\n";
  std::cout << "Speedup: "
            << (scalar_ms / (simd_ms > 0.001 ? simd_ms : 0.001)) << "x\n";
  std::cout << "Results match: " << (match ? "YES\n\n" : "NO\n\n");

  return match ? 0 : 1;
}
