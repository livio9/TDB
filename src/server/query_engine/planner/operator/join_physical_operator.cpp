#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "common/log/log.h"
#include "include/query_engine/structor/expression/value_expression.h"

JoinPhysicalOperator::JoinPhysicalOperator(std::unique_ptr<Expression> condition)
    : condition_(std::move(condition)) {
}

RC JoinPhysicalOperator::open(Trx *trx) {
  trx_ = trx;
  // Join operator should have exactly two children (left and right)
  if (children_.size() != 2) {
    return RC::INTERNAL;
  }
  left_child_ = children_[0].get();
  right_child_ = children_[1].get();
  // Open both child operators
  RC rc = left_child_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = right_child_->open(trx);
  if (rc != RC::SUCCESS) {
    left_child_->close();
    return rc;
  }
  current_left_tuple_ = nullptr;
  right_opened_ = true;
  return RC::SUCCESS;
}

RC JoinPhysicalOperator::next() {
  while (true) {
    // 若当前没有左侧 tuple，则获取下一个左侧 tuple
    if (current_left_tuple_ == nullptr) {
      RC rc_left = left_child_->next();
      if (rc_left != RC::SUCCESS) {
        // 若左侧tuple全部读完，则连接结束
        if (rc_left == RC::RECORD_EOF) {
          return RC::RECORD_EOF;
        }
        return rc_left;
      }
      current_left_tuple_ = left_child_->current_tuple();
      // 每次换左侧 tuple 时，需重新扫描右侧所有记录
      if (!right_opened_) {
        RC rc_reopen = right_child_->open(trx_);
        if (rc_reopen != RC::SUCCESS) {
          return rc_reopen;
        }
        right_opened_ = true;
      }
    }

    // 获取当前左侧 tuple 与右侧 tuple 的组合
    RC rc_right = right_child_->next();
    if (rc_right == RC::SUCCESS) {
      Tuple *right_tuple = right_child_->current_tuple();
      
      // 调用 JoinedTuple 的 set_left 和 set_right 组合当前记录
      joined_tuple_.set_left(current_left_tuple_);
      joined_tuple_.set_right(right_tuple);

      // 利用连接条件对组合的 tuple 进行评估
      bool match = true;
      if (condition_ != nullptr) {
        Value result;
        RC rc = condition_->get_value(joined_tuple_, result);
        if (rc != RC::SUCCESS) {
          return rc;
        }
        if (result.attr_type() == INTS) {
          match = (result.get_int() != 0);
        } else if (result.attr_type() == FLOATS) {
          match = (result.get_float() != 0.0f);
        } else {
          // 对于其他类型（如布尔值或字符串），调用 get_boolean() 判断真假
          match = result.get_boolean();
        }
      }
      if (match) {
        // 找到满足连接条件的组合记录
        return RC::SUCCESS;
      } else {
        // 如果不满足条件，则继续扫描右侧记录
        continue;
      }
    } else if (rc_right == RC::RECORD_EOF) {
      // 当前左侧 tuple 的右侧记录已全部扫描
      right_child_->close();
      right_opened_ = false;
      current_left_tuple_ = nullptr;
      // 移动到下一个左侧 tuple，并重新扫描右侧
      continue;
    } else {
      // 其他错误
      return rc_right;
    }
  }
}

RC JoinPhysicalOperator::close() {
  // Close both child operators
  RC rc1 = left_child_->close();
  RC rc2 = RC::SUCCESS;
  if (right_opened_) {
    rc2 = right_child_->close();
    right_opened_ = false;
  }
  current_left_tuple_ = nullptr;
  return (rc1 != RC::SUCCESS ? rc1 : rc2);
}

Tuple *JoinPhysicalOperator::current_tuple() {
  // Return the combined tuple of the current joined pair
  return &joined_tuple_;
}