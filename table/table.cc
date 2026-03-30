// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include <unordered_map>

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/bitweave.h"
#include "util/coding.h"

namespace leveldb {

// ---------------------------------------------------------------
// Table::Rep  — internal state for an open SST file
// ---------------------------------------------------------------
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;

  // ---- BitWeaving read-path state --------------------------------
  // bw_data owns the raw bytes of the BWH meta-block while this
  // Table is open. bw_reader points into those bytes.
  std::string   bw_data;    // empty → no BitWeaving index in this file
  BitWeaveReader bw_reader;

  // offset_to_block_index maps the byte offset of each data block
  // (as stored in the index block) to its sequential 0-based index.
  // Populated once in Table::Open by walking the index block.
  // Used by BlockReader to convert a handle offset into a tag index.
  std::unordered_map<uint64_t, uint32_t> offset_to_block_index;
  // ----------------------------------------------------------------
};

// ---------------------------------------------------------------
// Table::Open
// ---------------------------------------------------------------
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block.
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;

    *table = new Table(rep);
    (*table)->ReadMeta(footer);

    // ---- Build offset → block_index map from the index block --------
    // Walk every entry in the index block.  Each entry's value is a
    // varint-encoded BlockHandle; its offset field is the byte offset
    // of the corresponding data block in the file.
    {
      Iterator* index_iter =
          rep->index_block->NewIterator(options.comparator);
      uint32_t idx = 0;
      for (index_iter->SeekToFirst(); index_iter->Valid();
           index_iter->Next(), ++idx) {
        Slice handle_value = index_iter->value();
        BlockHandle handle;
        if (handle.DecodeFrom(&handle_value).ok()) {
          rep->offset_to_block_index[handle.offset()] = idx;
        }
      }
      delete index_iter;
    }
  }

  return s;
}

// ---------------------------------------------------------------
// ReadMeta  — load filter block and BitWeaving block from meta-index
// ---------------------------------------------------------------
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr &&
      rep_->bw_data.empty() /* not yet loaded */) {
    // No filter policy and we haven't tried loading BW yet.
    // Still fall through to load BitWeaving if present.
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation.
    return;
  }

  Block* meta = new Block(contents);
  Iterator* iter = meta->NewIterator(BytewiseComparator());

  // Load filter block (unchanged from stock LevelDB).
  if (rep_->options.filter_policy != nullptr) {
    std::string key = "filter.";
    key += rep_->options.filter_policy->Name();
    iter->Seek(key);
    if (iter->Valid() && iter->key() == Slice(key)) {
      ReadFilter(iter->value());
    }
  }

  // Load BitWeaving block.
  {
    iter->Seek(BW_BLOCK_NAME);
    if (iter->Valid() && iter->key() == Slice(BW_BLOCK_NAME)) {
      Slice handle_value = iter->value();
      BlockHandle bw_handle;
      if (bw_handle.DecodeFrom(&handle_value).ok()) {
        BlockContents bw_contents;
        if (ReadBlock(rep_->file, opt, bw_handle, &bw_contents).ok()) {
          // Copy bytes into rep_->bw_data so the reader has a stable buffer.
          rep_->bw_data.assign(bw_contents.data.data(),
                               bw_contents.data.size());
          if (bw_contents.heap_allocated) {
            delete[] bw_contents.data.data();
          }
          rep_->bw_reader.Init(rep_->bw_data.data(), rep_->bw_data.size());
        }
      }
    }
  }

  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() but let's
  // preserve the existing stock pattern exactly.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later.
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

// ---------------------------------------------------------------
// Table destructor / basic accessors
// ---------------------------------------------------------------
Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// ---------------------------------------------------------------
// BlockReader — the hot path; this is where we gate on MayMatch()
// ---------------------------------------------------------------
// Convert an index_value (encoded BlockHandle) into a Block iterator.
// If BitWeaving says this block cannot satisfy the predicate in
// options.bw_op / options.bw_predicate, return an empty iterator
// immediately without touching the disk.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);

  // ---- BitWeaving filter check ------------------------------------
  // Only evaluate if:
  //   1. We have a valid reader (the BWH block was found and parsed).
  //   2. The caller supplied a predicate (bw_op != 0).
  if (s.ok() && table->rep_->bw_reader.valid() &&
    options.bw_op != 0) {

 
  
  auto it = table->rep_->offset_to_block_index.find(handle.offset());
  if (it != table->rep_->offset_to_block_index.end()) {
    uint32_t block_index = it->second;
    bool may_match = table->rep_->bw_reader.MayMatch(block_index, options.bw_predicate, options.bw_op);
    if (!may_match) {
      return NewEmptyIterator();
    }
  } 
}
  // -----------------------------------------------------------------

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      // Try the block cache first.
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// ---------------------------------------------------------------
// NewIterator / InternalGet / ApproximateOffsetOf (stock, unchanged)
// ---------------------------------------------------------------
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found via bloom filter
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // temporary; this is good enough).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb