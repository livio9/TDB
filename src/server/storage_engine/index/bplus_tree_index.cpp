#include "include/storage_engine/index/bplus_tree_index.h"

BplusTreeIndex::~BplusTreeIndex() noexcept
{
  close();
}

RC BplusTreeIndex::create(const char *file_name, const IndexMeta &index_meta, const std::vector<FieldMeta> &multi_field_metas)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been inited before. file_name:%s, index:%s, field_amount:%d, field_names:%s",
             file_name,
             index_meta.name(),
             index_meta.field_amount(),
             index_meta.multi_fields());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, multi_field_metas);

  std::vector<AttrType> multi_attr_types;
  std::vector<int> multi_attr_length;
  for (int i = 0; i < multi_field_metas.size(); i++) {
    multi_attr_types.emplace_back(multi_field_metas[i].type());
    multi_attr_length.emplace_back(multi_field_metas[i].len());
  }

  RC rc = index_handler_.create(file_name, index_meta.is_unique(), multi_attr_types, multi_attr_length);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, field_amount:%d, field_names:%s rc:%s",
             file_name,
             index_meta.name(),
             index_meta.field_amount(),
             index_meta.multi_fields(),
             strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully create index, file_name:%s, index:%s, field_amount:%d, field_names:%s", file_name, index_meta.name(), index_meta.field_amount(), index_meta.multi_fields());
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(const char *file_name, const IndexMeta &index_meta, const std::vector<FieldMeta> &multi_field_metas)
{
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been inited before. file_name:%s, index:%s, field_amount:%d,  field_names:%s",
             file_name,
             index_meta.name(),
             index_meta.field_amount(),
             index_meta.multi_fields());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, multi_field_metas);

  RC rc = index_handler_.open(file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, field_amount:%d, field_names:%s, rc:%s",
             file_name,
             index_meta.name(),
             index_meta.field_amount(),
             index_meta.multi_fields(),
             strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully open index, file_name:%s, index:%s, field_amount:%d, field_names:%s", file_name, index_meta.name(), index_meta.field_amount(), index_meta.multi_fields());
  return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field_amount:%d, field_names:%s", index_meta_.name(), index_meta_.field_amount(), index_meta_.multi_fields());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

/**
 * 由于支持多字段索引，需要从record中取出multi_field_metas_中的字段值，作为key。
 * 需要调用BplusTreeHandler的insert_entry完成插入操作。
 * 注意如果是唯一索引（unique），需要判断是否存在重复的字段值，如果有，返回RECORD_DUPLICATE_KEY，插入失败。
 */
RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  // 构造索引键（组合多个字段的值）
  int key_length = 0;
  for (const FieldMeta &fm : multi_field_metas_) {
    key_length += fm.len();
  }
  char *key_data = new char[key_length];
  int offset = 0;
  for (const FieldMeta &fm : multi_field_metas_) {
    memcpy(key_data + offset, record + fm.offset(), fm.len());
    offset += fm.len();
  }

  // 如果是唯一索引，检查是否存在相同键值的记录
  if (index_meta_.is_unique()) {
    IndexScanner *scanner = create_scanner(key_data, key_length, true, key_data, key_length, true);
    if (scanner == nullptr) {
      delete[] key_data;
      return RC::INTERNAL;  // 索引扫描器创建失败
    }
    RID exist_rid;
    RC rc_scan = scanner->next_entry(&exist_rid, false);
    scanner->destroy();
    if (rc_scan == RC::SUCCESS) {
      // 已存在相同键值
      delete[] key_data;
      return RC::RECORD_DUPLICATE_KEY;
    }
    // 注意：如果rc_scan不是RC::SUCCESS，也不是因为找到记录，则可能是RC::RECORD_EOF表示无重复，继续插入
  }

  // 调用B+树索引处理器插入键值
   const char *key_ptr = key_data;  // 创建const char*指针
  RC rc = index_handler_.insert_entry(&key_ptr, rid);
  delete[] key_data;
  return rc;
}

/**
 * 由于支持多字段索引，需要从record中取出multi_field_metas_中的字段值，作为key。
 * 需要调用BplusTreeHandler的delete_entry完成插入操作。
 */
RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  // 构造索引键（组合多个字段的值）
  int key_length = 0;
  for (const FieldMeta &fm : multi_field_metas_) {
    key_length += fm.len();
  }
  char *key_data = new char[key_length];
  int offset = 0;
  for (const FieldMeta &fm : multi_field_metas_) {
    memcpy(key_data + offset, record + fm.offset(), fm.len());
    offset += fm.len();
  }

  // 调用B+树索引处理器删除键值
  const char *key_ptr = key_data;  // 创建const char*指针
  RC rc = index_handler_.delete_entry(&key_ptr, rid);
  delete[] key_data;
  return rc;
}

IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync()
{
  return index_handler_.sync();
}

////////////////////////////////////////////////////////////////////////////////

BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler)
{}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept
{
  tree_scanner_.close();
}

RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid, bool isdelete)
{
  return tree_scanner_.next_entry(*rid, isdelete);
}

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}