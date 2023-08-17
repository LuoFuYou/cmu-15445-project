//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void NestIndexJoinExecutor::Init() {
    outer_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
    outer_executor_->Init();

    auto inner_table_name = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->name_;
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_name);
    auto *index = index_info_->index_.get();
    b_plus_tree_index_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index);
    inner_table = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->table_.get();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) { 
    Tuple left_tuple;
    RID left_rid; 

    auto left_schema = plan_->OuterTableSchema();
    auto right_schema = plan_->InnerTableSchema();

    while (outer_executor_->Next(&left_tuple, &left_rid)) {
        Tuple key = left_tuple.KeyFromTuple(*left_schema, index_info_->key_schema_, index_info_->index_->GetKeyAttrs());
        std::vector<RID> result;

        b_plus_tree_index_->ScanKey(key, &result, exec_ctx_->GetTransaction());

        if (!result.empty()) {
            Tuple right_tuple;
            inner_table->GetTuple(result[0], &right_tuple, exec_ctx_->GetTransaction());

            auto predicate = plan_->Predicate();
            if (predicate == nullptr || 
                predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
                std::vector<Value> values;
                auto schema = plan_->OutputSchema();

                for (auto col : schema->GetColumns()) {
                    auto col_name = col.GetName();
                    int col_index;
                    try {
                        col_index = left_schema->GetColIdx(col_name);
                        values.emplace_back(left_tuple.GetValue(left_schema, col_index));
                    } catch(const std::exception &e) {
                        col_index = right_schema->GetColIdx(col_name);
                        values.emplace_back(right_tuple.GetValue(right_schema, col_index));
                    }
                }
                *tuple = Tuple(values, schema);

                return true;
            }
        }
    }
    
    return false;
}

}  // namespace bustub
