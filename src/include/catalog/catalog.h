#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");

    table_oid_t table_id = next_table_oid_++;
    TableHeap *table_heap = new TableHeap(bpm_, lock_manager_, log_manager_, txn);

    names_[table_name] = table_id;
    auto table_meta_data = new TableMetadata(schema, table_name, static_cast<std::unique_ptr<TableHeap>>(table_heap), table_id);
    tables_[table_id] = static_cast<std::unique_ptr<TableMetadata>>(table_meta_data);

    return table_meta_data;
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) { 
    if (names_.count(table_name) == 0) {
        throw std::out_of_range(table_name);
    }

    table_oid_t table_id = names_[table_name];

    return tables_[table_id].get();
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) { 
    if (tables_.count(table_oid) == 0) {
      throw std::out_of_range(std::to_string(table_oid));      
    }

    return tables_[table_oid].get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    auto index_oid = next_index_oid_++;
    auto index_meta_data = new IndexMetadata(index_name, table_name, &schema, key_attrs);
    std::unique_ptr<BPlusTreeIndex<KeyType, ValueType, KeyComparator>> b_plus_tree_index{new BPlusTreeIndex<KeyType, ValueType, KeyComparator>(index_meta_data, bpm_)} ;
    auto new_index = new IndexInfo(key_schema, index_name, std::move(b_plus_tree_index), index_oid, table_name, keysize);

    indexes_[index_oid] = static_cast<std::unique_ptr<IndexInfo>>(new_index);
    index_names_[table_name][index_name] = index_oid;

    auto table = GetTable(table_name)->table_.get();

    for (auto itr = table->Begin(txn); itr != table->End(); itr++) {
      new_index->index_->InsertEntry(itr->KeyFromTuple(schema, key_schema, key_attrs), itr->GetRid(), txn);
    }

    return new_index;
  }


  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) { 
    if (index_names_.count(table_name) == 0) {
      throw std::out_of_range(table_name);
    }

    index_oid_t index_oid = index_names_[table_name][index_name];
    
    return indexes_[index_oid].get();
  }

  IndexInfo *GetIndex(index_oid_t index_oid) { 
    if (indexes_.count(index_oid) == 0) {
      throw std::out_of_range(std::to_string(index_oid));
    }
    
    return indexes_[index_oid].get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> res; 
    for (auto item : index_names_[table_name]) {
      res.emplace_back(indexes_[item.second].get());
    }

    return res;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
