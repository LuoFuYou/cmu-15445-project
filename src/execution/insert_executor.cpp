//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
    TableMetadata *table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    table_heap_ = table_meta->table_.get();
    schema_ = &table_meta->schema_;

    if (plan_->IsRawInsert()) {
        has_inserted_ = false;
    }
    else {
        child_node_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
        child_node_->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_; 
    if (plan_->IsRawInsert()) {
        if (!has_inserted_) {
            for (auto values : plan_->RawValues()) {
                RID rid;
                table_heap_->InsertTuple(Tuple(values, schema_), &rid, exec_ctx_->GetTransaction());

                for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(table_name)) {
                    std::vector<Value> key;
                    for (auto col : index->key_schema_.GetColumns()) {
                        key.emplace_back(values[col.GetOffset()]);
                    }
                    index->index_->InsertEntry(Tuple(key, &(index->key_schema_)), rid, exec_ctx_->GetTransaction());
                }
            }

            has_inserted_ = true;
            return true;
        }
        
        return false;
    }
    else {
        if (child_node_->Next(tuple, rid)) {
            table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

            for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(table_name)) {
                Tuple key = tuple->KeyFromTuple(*schema_, index->key_schema_, index->index_->GetKeyAttrs());
                index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
            }

            return true;
        }

        return false;
    }
}

}  // namespace bustub
