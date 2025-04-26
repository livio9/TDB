#pragma once

#include <memory>
#include "logical_node.h"
#include "include/query_engine/structor/expression/expression.h"

// JoinLogicalNode: Logical plan node for join operations (inner join)
class JoinLogicalNode : public LogicalNode {
public:
  JoinLogicalNode() = default;
  ~JoinLogicalNode() override = default;

  LogicalNodeType type() const override {
    return LogicalNodeType::JOIN;
  }

  void set_condition(std::unique_ptr<Expression> &&condition) {
    condition_ = std::move(condition);
  }

  std::unique_ptr<Expression> &condition() {
    return condition_;
  }

private:
  // Join的条件，目前只支持等值连接 (join condition, only support equi-join)
  std::unique_ptr<Expression> condition_;
};