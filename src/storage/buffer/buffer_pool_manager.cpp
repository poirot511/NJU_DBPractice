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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {

  BufferPoolManager::BufferPoolManager(DiskManager* disk_manager, wsdb::LogManager* log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
  {
    if (REPLACER == "LRUReplacer") {
      replacer_ = std::make_unique<LRUReplacer>();
    }
    else if (REPLACER == "LRUKReplacer") {
      replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
    }
    else {
      WSDB_FETAL("Unknown replacer: " + REPLACER);
    }
    // init free_list_
    for (frame_id_t i = 0; i < static_cast<frame_id_t>(BUFFER_POOL_SIZE); i++) {
      free_list_.push_back(i);
    }

  }

  auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page* {
    std::lock_guard<std::mutex> guard(latch_);

    fid_pid_t key{ fid, pid };
    auto it = page_frame_lookup_.find(key);
    if (it != page_frame_lookup_.end()) {
      // 页面在缓冲池中
      frame_id_t frame_id = it->second;
      Frame& frame = frames_[frame_id];
      frame.Pin();
      replacer_->Pin(frame_id);
      return frame.GetPage();
    }

    // 页面不在缓冲池中
    frame_id_t frame_id = GetAvailableFrame();
    UpdateFrame(frame_id, fid, pid);
    Frame& frame = frames_[frame_id];
    return frame.GetPage();
  }

  auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
    std::lock_guard<std::mutex> guard(latch_);

    fid_pid_t key{ fid, pid };
    auto it = page_frame_lookup_.find(key);
    if (it == page_frame_lookup_.end()) {
      // 页面不在缓冲池中或未使用
      return false;
    }

    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (!frame.InUse()) {
      // 帧未被使用
      return false;
    }

    frame.Unpin();

    if (!frame.InUse()) {
      replacer_->Unpin(frame_id);
    }

    if (is_dirty) {
      frame.SetDirty(true);
    }

    return true;
  }

  auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool {
    std::lock_guard<std::mutex> guard(latch_);

    fid_pid_t key{ fid, pid };
    auto it = page_frame_lookup_.find(key);
    if (it == page_frame_lookup_.end()) {
      return true;
    }

    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (frame.InUse() > 0) {
      return false;
    }

    if (frame.IsDirty()) {
      // 直接执行flush逻辑，而不是调用FlushPage
      disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
      frame.SetDirty(false);
    }

    frame.Reset();
    free_list_.push_back(frame_id);
    replacer_->Unpin(frame_id);
    page_frame_lookup_.erase(it);

    return true;
  }

  auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    bool success = true;

    for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end();) {
      if (it->first.fid == fid) {
        frame_id_t frame_id = it->second;
        Frame& frame = frames_[frame_id];

        if (frame.InUse() > 0) {
          success = false;
          ++it;
          continue;
        }

        if (frame.IsDirty()) {
          disk_manager_->WritePage(fid, it->first.pid, frame.GetPage()->GetData());
          frame.SetDirty(false);
        }

        frame.Reset();
        free_list_.push_back(frame_id);
        replacer_->Unpin(frame_id);
        it = page_frame_lookup_.erase(it);
      }
      else {
        ++it;
      }
    }

    return success;
  }

  auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool {
    std::lock_guard<std::mutex> guard(latch_);

    fid_pid_t key{ fid, pid };
    auto it = page_frame_lookup_.find(key);
    if (it == page_frame_lookup_.end()) {
      // 页面不在缓冲池中
      return false;
    }

    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (frame.IsDirty()) {
      disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
      frame.SetDirty(false);
    }

    return true;
  }

  auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    bool success = true;

    for (const auto& entry : page_frame_lookup_) {
      if (entry.first.fid == fid) {
        frame_id_t frame_id = entry.second;
        Frame& frame = frames_[frame_id];

        if (frame.IsDirty()) {
          // 直接执行flush逻辑
          disk_manager_->WritePage(fid, entry.first.pid, frame.GetPage()->GetData());
          frame.SetDirty(false);
        }
      }
    }

    return success;
  }

  auto BufferPoolManager::GetAvailableFrame() -> frame_id_t {
    if (!free_list_.empty()) {
      frame_id_t frame_id = free_list_.front();
      free_list_.pop_front();
      return frame_id;
    }
    else {
      frame_id_t frame_id;
      if (replacer_->Victim(&frame_id)) {
        Frame& frame = frames_[frame_id];
        //在lookup中查找fid_pid_t

        for (const auto& entry : page_frame_lookup_) {
          if (entry.second == frame_id) {
            // 找到对应的{fid,pid}
            if (frame.IsDirty()) {
              disk_manager_->WritePage(entry.first.fid, entry.first.pid, frame.GetPage()->GetData());
              frame.SetDirty(false);
            }
            page_frame_lookup_.erase(entry.first);
            return frame_id;
          }
        }
        return frame_id;
      }
      else {
        WSDB_THROW(WSDB_NO_FREE_FRAME, "No free frame in buffer pool");
      }
    }
  }

  void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
    Frame& frame = frames_[frame_id];

    if (frame.IsDirty()) {
      disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
      frame.SetDirty(false);
    }

    frame.Reset();

    Page* page = frame.GetPage();
    page->SetTablePageId(fid, pid);
    disk_manager_->ReadPage(fid, pid, page->GetData());

    frame.Pin();
    replacer_->Pin(frame_id);

    page_frame_lookup_[{fid, pid}] = frame_id;
  }

  auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame*
  {
    const auto it = page_frame_lookup_.find({ fid, pid });
    return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
  }

}  // namespace wsdb
