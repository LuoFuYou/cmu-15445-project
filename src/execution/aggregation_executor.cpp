//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_{plan} {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
    child_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
    child_->Init();

    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
        aht_->InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
    }

    aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    auto schema = plan_->OutputSchema();

    while (*aht_iterator_ != aht_->End()) {
        auto key = (*aht_iterator_).Key().group_bys_;
        auto val = (*aht_iterator_).Val().aggregates_;
        ++(*aht_iterator_);

        auto predicate = plan_->GetHaving();
        if (predicate == nullptr ||
            predicate->EvaluateAggregate(key, val).GetAs<bool>()) {
            std::vector<Value> values;
            for (auto col : schema->GetColumns()) {
                values.emplace_back(col.GetExpr()->EvaluateAggregate(key, val));
            }
            
            *tuple = Tuple(values, schema);

            return true;
        }
    }

    return false;
}

}  // namespace bustub
