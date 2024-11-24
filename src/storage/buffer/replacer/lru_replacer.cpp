/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (lru_list_.empty()) {
    return false;
  }
  // 获取最近最少使用的可驱逐帧
  for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
    if (it->second) {
      *frame_id = it->first;
      lru_hash_.erase(it->first);
      lru_list_.erase(it);
      --cur_size_;
      return true;
    }
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = lru_hash_.find(frame_id);
  if (it != lru_hash_.end()) {
    if (it->second->second) {
      cur_size_--;
    }
    lru_list_.erase(it->second);
    lru_list_.emplace_back(frame_id, false);
    lru_hash_[frame_id] = --lru_list_.end();
  }
  else {
    lru_list_.emplace_back(frame_id, false);
    lru_hash_[frame_id] = --lru_list_.end();
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (lru_hash_.find(frame_id) == lru_hash_.end()) {
    lru_list_.emplace_back(frame_id, true);
    lru_hash_[frame_id] = --lru_list_.end();
  }
  else {
    auto frame_it = lru_hash_.find(frame_id);
    if(frame_it->second->second) {
      return;
    }
    frame_it->second->second = true;
    ++cur_size_;
  }
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return cur_size_;
}

}  // namespace wsdb
