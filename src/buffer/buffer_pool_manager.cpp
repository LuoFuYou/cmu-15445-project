//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();

  if (page_table_.count(page_id) != 0) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return page;
  }

  int addr = 0;
  frame_id_t *frame_id = &addr;
  if (!FindReplace(frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  Page *newPage = &pages_[*frame_id];
  disk_manager_->ReadPage(page_id, newPage->data_);
  newPage->page_id_ = page_id;
  newPage->pin_count_++;
  replacer_->Pin(*frame_id);
  newPage->is_dirty_ = false;
  page_table_[page_id] = *frame_id;
  latch_.unlock();

  return newPage;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  latch_.lock();

  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }

  if (--page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  latch_.unlock();
  return true;
 }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  
  latch_.lock();

  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->data_);

  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  
  latch_.lock();

  page_id_t newPageId = disk_manager_->AllocatePage();
  int addr = 0;
  frame_id_t *frame_id = &addr;
  if (!FindReplace(frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  Page *newPage = &pages_[*frame_id];
  newPage->page_id_ = newPageId;
  newPage->pin_count_++;
  replacer_->Pin(*frame_id);
  newPage->is_dirty_ = true;
  page_table_[newPageId] = *frame_id;
  *page_id = newPageId;

  latch_.unlock();
  return newPage;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  latch_.lock();

  if (page_table_.count(page_id) == 0) {
    disk_manager_->DeallocatePage(page_id);
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }

  page_table_.erase(page_id);
  disk_manager_->DeallocatePage(page_id);
  free_list_.push_back(frame_id);

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

bool BufferPoolManager::FindReplace(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
      *frame_id = free_list_.front();
      free_list_.pop_front();
      return true;
    }

  if (replacer_->Victim(frame_id)) {
      page_id_t replacePageId = -1;
      for (const auto &p : page_table_) {
        page_id_t pId = p.first;
        frame_id_t fid = p.second;
        if (fid == *frame_id) {
          replacePageId = pId;
          break;
        }
      }
      
      if (replacePageId != -1) {
        Page *replacePage = &pages_[*frame_id];
        if (replacePage->is_dirty_) {
          disk_manager_->WritePage(replacePageId, replacePage->data_);
        }
        page_table_.erase(replacePageId);
      }

      return true;
  }

  return false;
}

}  // namespace bustub
