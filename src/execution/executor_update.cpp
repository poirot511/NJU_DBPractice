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

#include "executor_update.h"

namespace wsdb {

UpdateExecutor::UpdateExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes,
    std::vector<std::pair<RTField, ValueSptr>> updates)
    : AbstractExecutor(DML),
      child_(std::move(child)),
      tbl_(tbl),
      indexes_(std::move(indexes)),
      updates_(std::move(updates)),
      is_end_(false)
{
  std::vector<RTField> fields(1);
  fields[0]   = RTField{.field_ = {.field_name_ = "updated", .field_size_ = sizeof(int), .field_type_ = TYPE_INT}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void UpdateExecutor::Init() { WSDB_FETAL("UpdateExecutor does not support Init"); }

void UpdateExecutor::Next() {
  if (is_end_) {
    return;
  }

  int count = 0;
  child_->Init();

  while (!child_->IsEnd()) {
    auto old_rec_ptr = child_->GetRecord();
    if (old_rec_ptr == nullptr) {
      child_->Next();
      continue;
    }
    
    // 获取原始记录的schema和RID
    const RecordSchema *schema = old_rec_ptr->GetSchema();
    RID old_rid = old_rec_ptr->GetRID();

    // 准备新记录的值
    std::vector<ValueSptr> new_values;
    new_values.reserve(schema->GetFieldCount());

    // 复制原记录的所有值
    for (size_t i = 0; i < schema->GetFieldCount(); i++) {
      new_values.push_back(old_rec_ptr->GetValueAt(i));
    }

    // 应用更新
    for (const auto &[field, new_value] : updates_) {
      try {
        size_t field_idx = schema->GetRTFieldIndex(field);
        if (field_idx < new_values.size()) {
          new_values[field_idx] = new_value;
        }
      } catch (const std::exception &e) {
        // 字段不存在，跳过这个更新
        continue;
      }
    }

    // 创建新记录
    Record new_rec(schema, new_values, old_rid);

    // 首先更新索引（如果更新失败可以提前返回）
    for (auto *idx : indexes_) {
      if (idx != nullptr) {
        idx->UpdateRecord(*old_rec_ptr, new_rec);
      }
    }

    // 更新表中的记录
    tbl_->UpdateRecord(old_rid, new_rec);
    count++;


    child_->Next();
  }

  // 创建结果记录
  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(count)};
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  is_end_ = true;
}

auto UpdateExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb