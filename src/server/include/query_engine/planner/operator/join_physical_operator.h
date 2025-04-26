#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"
#include "include/query_engine/structor/expression/expression.h"

// JoinPhysicalOperator: Physical operator for executing an inner join
class JoinPhysicalOperator : public PhysicalOperator {
public:
  JoinPhysicalOperator(std::unique_ptr<Expression> condition);
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  Trx *trx_ = nullptr;
  std::unique_ptr<Expression> condition_;
  PhysicalOperator *left_child_ = nullptr;
  PhysicalOperator *right_child_ = nullptr;
  Tuple *current_left_tuple_ = nullptr;
  bool right_opened_ = false;
  JoinedTuple joined_tuple_;  // 当前连接操作符输出的组合tuple
};