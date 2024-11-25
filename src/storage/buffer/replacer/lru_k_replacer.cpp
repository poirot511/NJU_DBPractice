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

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"

namespace wsdb {

    LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {
        cur_ts_ = 0;
        cur_size_ = 0;
    }

    auto LRUKReplacer::Victim(frame_id_t* frame_id) -> bool {
        std::lock_guard<std::mutex> guard(latch_);
        if (cur_size_ == 0) {
            return false;
        }

        timestamp_t cur_ts = cur_ts_;
        unsigned long long max_distance = 0;
        frame_id_t victim_frame = INVALID_FRAME_ID;
        bool has_less_than_k = false;
        timestamp_t earlist_ts = cur_ts + 1;
        // 第一遍遍历：查找访问次数小于k次的页面
        for (auto& it : node_store_) {
            auto& node = it.second;
            if (!node.IsEvictable()) {
                continue;
            }
            if (node.GetHistorySize() < k_) {
                has_less_than_k = true;
                if (node.GetFirstAccessTime() < earlist_ts) {
                    earlist_ts = node.GetFirstAccessTime();
                    victim_frame = it.first;
                }
            }
        }

        // 如果没有找到访问次数小于k次的页面，则查找访问次数达到k次的页面
        if (!has_less_than_k) {
            for (auto& it : node_store_) {
                auto& node = it.second;
                if (!node.IsEvictable() || node.GetHistorySize() < k_) {
                    continue;
                }
                unsigned long long distance = node.GetBackwardKDistance(cur_ts);
                if (distance > max_distance) {
                    max_distance = distance;
                    victim_frame = it.first;
                }
            }
        }

        if (victim_frame != INVALID_FRAME_ID) {
            node_store_.erase(victim_frame);
            *frame_id = victim_frame;
            --cur_size_;
            return true;
        }
        return false;
    }

    void LRUKReplacer::Pin(frame_id_t frame_id) {
        std::lock_guard<std::mutex> guard(latch_);
        timestamp_t cur_ts = ++cur_ts_;  // 在 Pin 时递增 cur_ts_
        auto it = node_store_.find(frame_id);
        if (it == node_store_.end()) {
            LRUKNode node(frame_id, k_);
            node.AddHistory(cur_ts);
            node.SetEvictable(false);
            node_store_[frame_id] = node;
        }
        else {
            auto& node = it->second;
            node.AddHistory(cur_ts);
            if (node.IsEvictable()) {
                node.SetEvictable(false);
                --cur_size_;
            }
        }
    }

    void LRUKReplacer::Unpin(frame_id_t frame_id) {
        std::lock_guard<std::mutex> guard(latch_);
        auto it = node_store_.find(frame_id);
        if (it != node_store_.end()) {
            auto& node = it->second;
            if (!node.IsEvictable()) {
                node.SetEvictable(true);
                ++cur_size_;
            }
        }
    }

    auto LRUKReplacer::Size() -> size_t {
        std::lock_guard<std::mutex> guard(latch_);
        return cur_size_;
    }

}  // namespace wsdb
