//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    TableMetadata *table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    table_heap_ = table_meta->table_.get();
    schema_ = &table_meta->schema_;
    itr_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    while (*itr_ != table_heap_->End()) {
        *tuple = **itr_;
        ++(*itr_);
        *rid = tuple->GetRid();

        const auto &predicate = plan_->GetPredicate();
        if (predicate == nullptr ||
            predicate->Evaluate(tuple, schema_).GetAs<bool>()) {
            std::vector<Value> values;
            for (auto &col : plan_->OutputSchema()->GetColumns()) {
                values.emplace_back(tuple->GetValue(schema_, schema_->GetColIdx(col.GetName())));
            }

            *tuple = Tuple(values, plan_->OutputSchema());

            return true;
        }
    }

    return false;
}

}  // namespace bustub
