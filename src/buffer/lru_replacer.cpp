//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {capacity = num_pages;}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    latch.lock();
    if (lruMap.empty()) {
        latch.unlock();
        return false;
    }

    *frame_id = lruList.back();
    lruList.pop_back();
    lruMap.erase(*frame_id);
    latch.unlock();
    return true;
 }

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch.lock();
    if (lruMap.count(frame_id) == 0) {
        latch.unlock();
        return;
    }

    lruList.erase(lruMap[frame_id]);
    lruMap.erase(frame_id);
    latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch.lock();
    if (lruMap.count(frame_id) != 0) {
        latch.unlock();
        return;
    }

    if (Size() >= capacity) {
        frame_id_t delId = lruList.front();
        lruList.pop_front();
        lruMap.erase(delId);
    }

    lruList.push_front(frame_id);
    lruMap[frame_id] = lruList.begin();
    latch.unlock();
}

size_t LRUReplacer::Size() { return lruList.size(); }

}  // namespace bustub
