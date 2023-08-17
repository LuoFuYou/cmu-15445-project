//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void DeleteExecutor::Init() {
    TableMetadata *table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    table_heap_ = table_meta->table_.get();
    schema_ = &table_meta->schema_;

    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    if (child_executor_->Next(tuple, rid)) {
        table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction());

        std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
        for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(table_name)) {
            Tuple key = tuple->KeyFromTuple(*schema_, index->key_schema_, index->index_->GetKeyAttrs());
            index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
        }

        return true;
    }

    return false;
}

}  // namespace bustub
