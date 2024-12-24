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

#include "executor_filter.h"

namespace wsdb {

FilterExecutor::FilterExecutor(AbstractExecutorUptr child, std::function<bool(const Record &)> filter)
    : AbstractExecutor(Basic), child_(std::move(child)), filter_(std::move(filter))
{}

void FilterExecutor::Init() {
    child_->Init();
    // 初始化时就开始寻找第一个满足条件的记录
    while (!child_->IsEnd()) {
        auto rec = child_->GetRecord();
        if (rec && filter_(*rec)) {
            record_ = std::move(rec);
            return;
        }
        child_->Next();
    }
}

void FilterExecutor::Next() {
    if (child_->IsEnd()) {
        record_ = nullptr;
        return;
    }
    
    child_->Next();
    // 查找下一个满足过滤条件的记录
    while (!child_->IsEnd()) {
        auto rec = child_->GetRecord();
        if (rec && filter_(*rec)) {
            record_ = std::move(rec);
            return;
        }
        child_->Next();
    }
    record_ = nullptr;
}

auto FilterExecutor::IsEnd() const -> bool {
    return record_ == nullptr;
}

auto FilterExecutor::GetOutSchema() const -> const RecordSchema * { return child_->GetOutSchema(); }
}  // namespace wsdb
