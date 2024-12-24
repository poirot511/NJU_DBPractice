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

#include "executor_projection.h"

namespace wsdb {

ProjectionExecutor::ProjectionExecutor(AbstractExecutorUptr child, RecordSchemaUptr proj_schema)
    : AbstractExecutor(Basic), child_(std::move(child))
{
  out_schema_ = std::move(proj_schema);
}

// hint: record_ = std::make_unique<Record>(out_schema_.get(), *child_record);

void ProjectionExecutor::Init() {
    // 初始化子执行器
    child_->Init();   
    // 清空当前记录
    record_ = nullptr;   
    // 如果子执行器不为空且有数据，则创建第一条投影记录
    if (!child_->IsEnd()) {
        RecordUptr child_record = child_->GetRecord();
        if (child_record != nullptr) {
            record_ = std::make_unique<Record>(out_schema_.get(), *child_record);
        }
    }
}

void ProjectionExecutor::Next() {
    // 清空当前记录
    record_ = nullptr;   
    // 获取子执行器的下一条记录
    child_->Next();  
    // 如果子执行器还有数据，创建新的投影记录
    if (!child_->IsEnd()) {
        RecordUptr child_record = child_->GetRecord();
        if (child_record != nullptr) {
            record_ = std::make_unique<Record>(out_schema_.get(), *child_record);
        }
    }
}

auto ProjectionExecutor::IsEnd() const -> bool {
    // 如果子执行器已结束或当前记录为空，则认为投影执行器已结束
    return child_->IsEnd() || record_ == nullptr;
}

}  // namespace wsdb