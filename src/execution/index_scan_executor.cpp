//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
// INDEX_TEMPLATE_ARGUMENTS
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// INDEX_TEMPLATE_ARGUMENTS
void IndexScanExecutor::Init() {
    auto *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    auto *index = index_info->index_.get();
    b_plus_tree_index_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index);
    itr_ = std::make_unique<IndexIterator<GenericKey<8>, RID, GenericComparator<8>>>(b_plus_tree_index_->GetBeginIterator());

    TableMetadata *table_meta = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
    table_heap_ = table_meta->table_.get();
    schema_ = &table_meta->schema_;
}

// INDEX_TEMPLATE_ARGUMENTS
bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) { 
    while (*itr_ != b_plus_tree_index_->GetEndIterator()) {
        *rid = (**itr_).second;
        table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
        ++(*itr_);

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
