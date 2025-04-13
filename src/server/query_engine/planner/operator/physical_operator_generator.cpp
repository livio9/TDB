#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"
#include "common/log/log.h"
#include "include/storage_engine/recorder/table.h"
#include "include/query_engine/structor/expression/comparison_expression.h"   // ★ 新增
#include "include/query_engine/structor/expression/field_expression.h"        // ★ 新增
#include "include/query_engine/structor/expression/value_expression.h"        // ★ 新增
#include "include/query_engine/planner/operator/index_scan_physical_operator.h"
#include "include/storage_engine/index/index.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator), oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN:
    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2] 
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
// 首先检查扫描的table是否存在索引，如果存在可以使用的索引，那么我们可以直接生成IndexScanOperator来减少磁盘的扫描
RC PhysicalOperatorGenerator::create_plan(
    TableGetLogicalNode &table_get_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Index *index = nullptr;
  Value equality_value;
  string field_name;
  // 遍历等值比较谓词，寻找可利用的索引
  for (size_t i = 0; i < predicates.size(); ++i) {
    Expression *expr = predicates[i].get();
    if (expr->type() != ExprType::COMPARISON) {
      continue;
    }
    ComparisonExpr *comp_expr = static_cast<ComparisonExpr*>(expr);
    if (comp_expr->comp() != CompOp::EQUAL_TO) {
      continue;
    }
    // 检查左右两侧是否为 FieldExpr 和 ValueExpr 的组合
    Expression *left = comp_expr->left().get();
    Expression *right = comp_expr->right().get();
    FieldExpr *field_expr = nullptr;
    ValueExpr *value_expr = nullptr;
    if (left->type() == ExprType::FIELD && right->type() == ExprType::VALUE) {
      field_expr = static_cast<FieldExpr*>(left);
      value_expr = static_cast<ValueExpr*>(right);
    } else if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
      field_expr = static_cast<FieldExpr*>(right);
      value_expr = static_cast<ValueExpr*>(left);
    } else {
      continue;
    }
    // 确认字段属于当前扫描的表
    if (strcmp(field_expr->table_name(), table_get_oper.table()->name()) != 0) {
      continue;
    }
    // 查找该字段对应的索引
    index = table_get_oper.table()->find_index_by_field(field_expr->field_name());
    if (index == nullptr) {
      continue;
    }
    // 找到可用索引，记录等值比较的常量值并移除该谓词
    equality_value = value_expr->get_value();
    // 移除已用于索引的谓词表达式
    predicates.erase(predicates.begin() + i);
    break;
  }

  if (index == nullptr) {
    // 未找到可用索引，使用全表扫描
    Table *table = table_get_oper.table();
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.table_alias(), table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  } else {
    // 使用索引扫描算子
    Table *table = table_get_oper.table();
    IndexScanPhysicalOperator *index_scan_oper = new IndexScanPhysicalOperator(table, index, table_get_oper.readonly(),
                                                                              &equality_value, true, &equality_value, true);
    index_scan_oper->isdelete_ = is_delete;
    index_scan_oper->set_table_alias(table_get_oper.table_alias());
    index_scan_oper->set_predicates(predicates);
    oper = unique_ptr<PhysicalOperator>(index_scan_oper);
    LOG_TRACE("use index scan on %s", index->index_meta().name());
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(
    PredicateLogicalNode &pred_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode &aggr_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = aggr_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *aggr_operator = new AggrPhysicalOperator(&aggr_oper);

  if (child_phy_oper) {
    aggr_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(aggr_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode &order_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = order_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  OrderPhysicalOperator* order_operator = new OrderPhysicalOperator(std::move(order_oper.order_units()));

  if (child_phy_oper) {
    order_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(order_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ProjectLogicalNode &project_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *project_operator = new ProjectPhysicalOperator(&project_oper);
  for (const auto &i : project_oper.expressions()) {
    // TupleCellSpec 的构造函数中已经 copy 了 Expression，这里无需 copy
    project_operator->add_projector(i.get());
  }

  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(project_operator);
  oper->isdelete_ = is_delete;

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode &insert_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table *table = insert_oper.table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_oper.multi_values().size(); i++) {
    vector<Value> &values = insert_oper.values(i);
    multi_values.push_back(values);
  }
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(multi_values));
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode &delete_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));
  oper->isdelete_ = true;
  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode &update_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 将 update_units 从逻辑算子转移给物理算子，避免重复释放
  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(update_oper.table(), std::move(update_oper.update_units())));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ExplainLogicalNode &explain_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalNode> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  oper->isdelete_ = is_delete;
  return rc;
}

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(
    JoinLogicalNode &join_oper, unique_ptr<PhysicalOperator> &oper)
{
  return RC::UNIMPLENMENT;
}