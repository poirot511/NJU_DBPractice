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
// Created by ziqi on 2024/7/18.
//

#include "executor_seqscan.h"

namespace wsdb {

SeqScanExecutor::SeqScanExecutor(TableHandle *tab) : AbstractExecutor(Basic), tab_(tab) {}

void SeqScanExecutor::Init() {
    // 初始化RID和结束标志
    rid_ = tab_->GetFirstRID();
    is_end_ = (rid_ == INVALID_RID);
    record_ = nullptr;

    // 如果存在有效记录，获取第一条记录
    if (!is_end_) {
        record_ = tab_->GetRecord(rid_);
        // 如果获取记录失败，标记为结束
        if (record_ == nullptr) {
            is_end_ = true;
        }
    }
}

void SeqScanExecutor::Next() {
    // 清空当前记录
    record_ = nullptr;
    
    // 如果未结束，获取下一条记录
    if (!is_end_) {
        // 获取下一个RID
        rid_ = tab_->GetNextRID(rid_);
        
        // 检查是否到达末尾
        if (rid_ == INVALID_RID) {
            is_end_ = true;
        } else {
            // 获取下一条记录
            record_ = tab_->GetRecord(rid_);
            // 如果获取记录失败，标记为结束
            if (record_ == nullptr) {
                is_end_ = true;
            }
        }
    }
}

auto SeqScanExecutor::IsEnd() const -> bool {
    // 同时检查结束标志和记录状态
    return is_end_ || record_ == nullptr;
}

auto SeqScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tab_->GetSchema(); }


}  // namespace wsdb