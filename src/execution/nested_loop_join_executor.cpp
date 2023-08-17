//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void NestedLoopJoinExecutor::Init() {
    left_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetLeftPlan());
    left_executor_->Init();
    right_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetRightPlan());
    right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple left_tuple;
    RID left_rid; 
    while (left_executor_->Next(&left_tuple, &left_rid)) {
        Tuple right_tuple;
        RID right_rid;
        while (right_executor_->Next(&right_tuple, &right_rid)) {
            auto left_schema = left_executor_->GetOutputSchema();
            auto right_schema = right_executor_->GetOutputSchema();
            
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
        right_executor_->Init();
    }

    return false;
}

}  // namespace bustub
