#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "util/bitweave.h"

namespace leveldb {

// -------------------------------------------------------
// Unit tests for BitWeaveBuilder + BitWeaveReader
// -------------------------------------------------------

TEST(BitWeaveBuilderReader, SingleBlockAllMatch) {
  BitWeaveBuilder b;
  for (int i = 0; i < 100; i++) b.AddValue(i * 10);  // 0..990
  b.FlushBlock();

  std::string out;
  b.Finish(&out);

  BitWeaveReader r;
  ASSERT_TRUE(r.Init(out.data(), out.size()));
  ASSERT_EQ(r.num_blocks(), 1u);

  // Predicate: value > 500 — block contains values up to 990, should match
  EXPECT_TRUE(r.MayMatch(0, 500, '>'));
}

TEST(BitWeaveBuilderReader, SingleBlockAllSkip) {
  BitWeaveBuilder b;
  for (int i = 0; i < 50; i++) b.AddValue(i);  // 0..49
  b.FlushBlock();

  std::string out;
  b.Finish(&out);

  BitWeaveReader r;
  ASSERT_TRUE(r.Init(out.data(), out.size()));

  // Predicate: value > 900 — block max is 49, must skip
  EXPECT_FALSE(r.MayMatch(0, 900, '>'));
}

TEST(BitWeaveBuilderReader, MultiBlock) {
  BitWeaveBuilder b;

  // Block 0: values 0..99
  for (int i = 0; i < 100; i++) b.AddValue(i);
  b.FlushBlock();

  // Block 1: values 900..999
  for (int i = 900; i < 1000; i++) b.AddValue(i);
  b.FlushBlock();

  std::string out;
  b.Finish(&out);

  BitWeaveReader r;
  ASSERT_TRUE(r.Init(out.data(), out.size()));
  ASSERT_EQ(r.num_blocks(), 2u);

  // value > 800: block 0 (max=99) must skip, block 1 (min=900) must match
  EXPECT_FALSE(r.MayMatch(0, 800, '>'));
  EXPECT_TRUE (r.MayMatch(1, 800, '>'));

  // value < 50: block 0 must match, block 1 (min=900) must skip
  EXPECT_TRUE (r.MayMatch(0, 50, '<'));
  EXPECT_FALSE(r.MayMatch(1, 50, '<'));
}

TEST(BitWeaveBuilderReader, NoFalseNegatives) {
  // Every value that IS in a block must never be skipped
  BitWeaveBuilder b;
  for (int v = 0; v <= 1000; v += 7) b.AddValue(v);
  b.FlushBlock();

  std::string out;
  b.Finish(&out);

  BitWeaveReader r;
  ASSERT_TRUE(r.Init(out.data(), out.size()));

  // For every value actually in the block, MayMatch(value-1, '>') must be true
  for (int v = 0; v <= 1000; v += 7) {
    if (v > 0) {
      EXPECT_TRUE(r.MayMatch(0, (uint32_t)(v - 1), '>'))
          << "False negative for value " << v;
    }
  }
}

// -------------------------------------------------------
// Integration test: write a real SST via DB, scan with
// predicate, confirm result count is correct
// -------------------------------------------------------
TEST(BitWeaveIntegration, ScanWithPredicate) {
  std::string dbpath = testing::TempDir() + "bw_integration_test";
  DestroyDB(dbpath, Options());

  Options opts;
  opts.create_if_missing = true;
  opts.block_size = 1024;  // Force multiple blocks
  DB* db;
  ASSERT_TRUE(DB::Open(opts, dbpath, &db).ok());

  // Write 10000 records; value is i % 1000
  WriteOptions wo;
  int expected_true_matches = 0;
  for (int i = 0; i < 10000; i++) {
    char key[32], val[32];
    snprintf(key, sizeof(key), "key%08d", i);
    snprintf(val, sizeof(val), "%d", i % 1000);
    db->Put(wo, key, val);
    if ((i % 1000) > 800) expected_true_matches++;
  }

  db->CompactRange(nullptr, nullptr);

  // Scan WITH BitWeaving predicate
  ReadOptions ro_with_bw;
  ro_with_bw.bw_predicate = 800;
  ro_with_bw.bw_op = '>';

  Iterator* it_bw = db->NewIterator(ro_with_bw);
  int found_with_bw = 0;
  int true_matches_with_bw = 0;
  for (it_bw->SeekToFirst(); it_bw->Valid(); it_bw->Next()) {
    found_with_bw++;
    int v = atoi(it_bw->value().data());
    if (v > 800) true_matches_with_bw++;
  }
  delete it_bw;

  // Scan WITHOUT BitWeaving (baseline)
  ReadOptions ro_no_bw;  // bw_op = 0 (disabled)
  Iterator* it_no_bw = db->NewIterator(ro_no_bw);
  int found_no_bw = 0;
  for (it_no_bw->SeekToFirst(); it_no_bw->Valid(); it_no_bw->Next()) {
    found_no_bw++;
  }
  delete it_no_bw;

  delete db;
  DestroyDB(dbpath, Options());

  // Verify BitWeaving results
  EXPECT_EQ(true_matches_with_bw, expected_true_matches) 
      << "All true matches should be found";
  EXPECT_LT(found_with_bw, found_no_bw) 
      << "BitWeaving should reduce records scanned";
  EXPECT_GT(found_with_bw, expected_true_matches) 
      << "Some false positives are acceptable";
  
  std::cout << "Records scanned (no BW): " << found_no_bw << std::endl;
  std::cout << "Records scanned (with BW): " << found_with_bw << std::endl;
  std::cout << "True matches: " << true_matches_with_bw << std::endl;
  std::cout << "I/O reduction: " << (100.0 * (found_no_bw - found_with_bw) / found_no_bw) 
            << "%" << std::endl;
}

}  // namespace leveldb