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
// Created by ziqi on 2024/8/5.
//
#include <unistd.h>
#include "common/config.h"
#include "executor_sort.h"

static long long sort_result_fresh_id_ = 0;
#define SORT_FILE_PATH(obj_name) FILE_NAME(TMP_DIR, obj_name, TMP_SUFFIX)

namespace wsdb {
SortExecutor::SortExecutor(AbstractExecutorUptr child, RecordSchemaUptr key_schema, bool is_desc)
    : AbstractExecutor(Basic),
      child_(std::move(child)),
      key_schema_(std::move(key_schema)),
      buf_idx_(0),
      is_desc_(is_desc),
      is_sorted_(false),
      is_merge_sort_(false),
      max_rec_num_(SORT_BUFFER_SIZE / child_->GetOutSchema()->GetRecordLength()),
      tmp_file_num_(0),
      merge_result_file_(fmt::format("sort_result_{}", sort_result_fresh_id_++))
{
  // comment the line below after testing
  //  max_rec_num_ = 10;
}

SortExecutor::~SortExecutor()
{
  if (is_merge_sort_) {
    // TODO: do some clean up, e.g. close the result file
    WSDB_STUDENT_TODO(l2, f1);
  }
}

void SortExecutor::Init() {
  // TODO: decide whether to use merge sort according to the cardinality of the child. 
  is_merge_sort_ = false;
  if (is_merge_sort_) {
    WSDB_STUDENT_TODO(l2, f1);
    return;
  }
  buf_idx_ = 0;
  is_sorted_ = false;
  sort_buffer_.clear();
  child_->Init();
  while (!child_->IsEnd()) {
    auto record = child_->GetRecord();
    if (record != nullptr) {
      sort_buffer_.push_back(std::move(record));
    }
    child_->Next();
  }
  if (!sort_buffer_.empty()) {
    SortBuffer();
    is_sorted_ = true;
    record_ = std::make_unique<Record>(*sort_buffer_[0]);
  }
}

void SortExecutor::Next() {
  if (is_merge_sort_) {
    WSDB_STUDENT_TODO(L2, f1);
    return;
  }

  if (!is_sorted_ || buf_idx_ >= sort_buffer_.size() - 1) {
    record_ = nullptr;
    return;
  }

  buf_idx_++;
  if (buf_idx_ < sort_buffer_.size()) {
    record_ = std::make_unique<Record>(*sort_buffer_[buf_idx_]);
  } else {
    record_ = nullptr;
  }
}

auto SortExecutor::IsEnd() const -> bool {
  if (is_merge_sort_) {
    WSDB_STUDENT_TODO(L2, f1);
    return true;
  }
  
  return !is_sorted_ || record_ == nullptr || buf_idx_ >= sort_buffer_.size();
}

auto SortExecutor::Compare(const Record &lhs, const Record &rhs) const -> bool
{
  auto lkey = std::make_unique<Record>(key_schema_.get(), lhs);
  auto rkey = std::make_unique<Record>(key_schema_.get(), rhs);
  return is_desc_ ? Record::Compare(*lkey, *rkey) > 0 : lkey->Compare(*lkey, *rkey) < 0;
}

auto SortExecutor::GetOutSchema() const -> const RecordSchema * { return child_->GetOutSchema(); }

/// methods below are only used for merge sort

auto SortExecutor::GetSortFileName(size_t file_group, size_t file_idx) const -> std::string
{
  return fmt::format("{}_{}_{}", merge_result_file_, file_group, file_idx);
}

void SortExecutor::SortBuffer() {
  std::sort(sort_buffer_.begin(), sort_buffer_.end(), 
    [this](const RecordUptr& a, const RecordUptr& b) {
      return this->Compare(*a, *b);
    });
}

void SortExecutor::DumpBufferToFile(size_t file_idx) { WSDB_STUDENT_TODO(L2, f1); }

void SortExecutor::LoadMergeResult() { WSDB_STUDENT_TODO(L2, f1); }

void SortExecutor::Merge()
{
  // 1. create a heap according to is_desc_
  // 2. read the first tuple from each file
  // 3. pop the top of the heap and write to file: group 1, file: file_index
  // run until all runs are exhausted, then read tuples from group 1, file 0, write to group 0, file 0 ...
  WSDB_STUDENT_TODO(L2, f1);
}

}  // namespace wsdb