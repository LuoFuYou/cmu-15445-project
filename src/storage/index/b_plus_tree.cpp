//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPageRW(key, OpType::READ, false, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage*>(page);

  if (transaction != nullptr) {
    page->RLatch();
    UnLatchAndUnpin(OpType::READ, transaction);
  }

  RID rid;
  bool res = leaf_page->Lookup(key, &rid, comparator_);
  (*result).push_back(rid);

  if (transaction != nullptr) {
    page->RUnlatch();
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  latch_.lock();

  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    latch_.unlock();
    throw ("out of memory");
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  UpdateRootPageId(1);

  buffer_pool_manager_->UnpinPage(root_page_id_, true);

  latch_.unlock();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);

    return true;
  }

  Page *page = FindLeafPageRW(key, OpType::INSERT, false, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);

  if (transaction != nullptr) {
    page->WLatch();
    if (leaf_page->IsSafe(OpType::INSERT)) {
      UnLatchAndUnpin(OpType::INSERT, transaction);
    }
  }

  ValueType val;
  bool res = true;
  if (leaf_page->Lookup(key, &val, comparator_)) {
    res = false;
  }
  else {
    leaf_page->Insert(key, value, comparator_);

    if (leaf_page->GetSize() > leaf_page->GetMaxSize() - 1) {
      LeafPage *new_leaf_page = Split(leaf_page);
      InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);

      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    }
  }

  if (transaction != nullptr) {
    UnLatchAndUnpin(OpType::INSERT, transaction);
    page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

  return res;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id = -1;
  Page *page = buffer_pool_manager_->NewPage(&page_id);

  if (page == nullptr) {
    throw("out of memory");
  }

  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(page);
    new_leaf_page->Init(page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf_page, comparator_);

    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());

    N *new_node = reinterpret_cast<N *>(new_leaf_page);
    return new_node;
  }
  else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_page = reinterpret_cast<InternalPage *>(page);
    new_internal_page->Init(page_id, internal_page->GetParentPageId(), internal_max_size_);
    internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);

    N *new_node = reinterpret_cast<N *>(new_internal_page);
    return new_node;
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_root_id = -1;
    Page *page = buffer_pool_manager_->NewPage(&new_root_id);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(page);
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = new_root_id;
    UpdateRootPageId(2);

    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }
  else {
    Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(page);
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    new_node->SetParentPageId(old_node->GetParentPageId());

    if (parent_node->GetSize() > parent_node->GetMaxSize()) {
      InternalPage *new_parent_node = Split(parent_node);
      InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node);

      buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) { 
  Page *page = FindLeafPageRW(key, OpType::DELETE, false, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);

  if (transaction != nullptr) {
    page->WLatch();
    if (leaf_page->IsSafe(OpType::DELETE)) {
      UnLatchAndUnpin(OpType::DELETE, transaction);
    }
  }

  if (leaf_page->GetSize() != leaf_page->RemoveAndDeleteRecord(key, comparator_)) {
    int index = leaf_page->KeyIndex(key, comparator_);
    page_id_t parent_page_id = leaf_page->GetParentPageId();
    if (index == 0 && parent_page_id > 0) {
      Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
      InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);

      int parent_index = parent_page->KeyIndex(key, comparator_);
      if (parent_index >= 0) {
        parent_page->SetKeyAt(parent_index, leaf_page->KeyAt(0));
      }

      buffer_pool_manager_->UnpinPage(parent_page_id, true);
    }

    if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
      CoalesceOrRedistribute(leaf_page);
    }
  }

  if (transaction != nullptr) {
    UnLatchAndUnpin(OpType::DELETE, transaction);
    page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(node);

  if ( b_plus_tree_page->GetPageId() == root_page_id_) {
    return AdjustRoot(b_plus_tree_page);
  }

  if (b_plus_tree_page->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(b_plus_tree_page);

    page_id_t parent_id = leaf_page->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);

    int index = parent_page->ValueIndex(leaf_page->GetPageId());

    if (index + 1 < parent_page->GetSize()) {
      page_id_t right_sibling_id = parent_page->ValueAt(index + 1);
      page = buffer_pool_manager_->FetchPage(right_sibling_id);
      LeafPage *right_sibling_page = reinterpret_cast<LeafPage *>(page);
      if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
        page->WLatch();

        Redistribute(right_sibling_page, leaf_page, 0);

        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);

        return true;
      }
    }

    if (index - 1 >= 0) {
      page_id_t left_sibling_id = parent_page->ValueAt(index - 1);
      page = buffer_pool_manager_->FetchPage(left_sibling_id);
      LeafPage *left_sibling_page = reinterpret_cast<LeafPage *>(page);
      if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
        page->WLatch();

        Redistribute(left_sibling_page, leaf_page, 1);

        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);

        return true;
      }
    }

    if (index - 1 >= 0) {
      page_id_t left_sibling_id = parent_page->ValueAt(index - 1);
      page = buffer_pool_manager_->FetchPage(left_sibling_id);
      LeafPage *left_sibling_page = reinterpret_cast<LeafPage *>(page);

      page->WLatch();

      Coalesce(&left_sibling_page, &leaf_page, &parent_page, index, transaction);

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
    }
    else {
      page_id_t right_sibling_id = parent_page->ValueAt(index + 1);
      page = buffer_pool_manager_->FetchPage(right_sibling_id);
      LeafPage *right_sibling_page = reinterpret_cast<LeafPage *>(page);

      page->WLatch();

      Coalesce(&leaf_page, &right_sibling_page, &parent_page, index, transaction);

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    return true;
  } 
  else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);

    page_id_t parent_id = internal_page->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);

    int index = parent_page->ValueIndex(internal_page->GetPageId());

    if (index + 1 < parent_page->GetSize()) {
      page_id_t right_sibling_id = parent_page->ValueAt(index + 1);
      page = buffer_pool_manager_->FetchPage(right_sibling_id);
      InternalPage *right_sibling_page = reinterpret_cast<InternalPage *>(page);
      if (right_sibling_page->GetSize() - 1 > right_sibling_page->GetMinSize()) {
        page->WLatch();

        Redistribute(right_sibling_page, internal_page, 0);

        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);

        return true;
      }
    }

    if (index - 1 >= 0) {
      page_id_t left_sibling_id = parent_page->ValueAt(index - 1);
      page = buffer_pool_manager_->FetchPage(left_sibling_id);
      InternalPage *left_sibling_page = reinterpret_cast<InternalPage *>(page);
      if (left_sibling_page->GetSize() - 1 > left_sibling_page->GetMinSize()) {
        page->WLatch();

        Redistribute(left_sibling_page, internal_page, 1);

        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);

        return true;
      }
    }

    if (index - 1 >= 0) {
      page_id_t left_sibling_id = parent_page->ValueAt(index - 1);
      page = buffer_pool_manager_->FetchPage(left_sibling_id);
      InternalPage *left_sibling_page = reinterpret_cast<InternalPage *>(page);
      page->WLatch();

      Coalesce(&left_sibling_page, &internal_page, &parent_page, index, transaction);

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
    }
    else {
      page_id_t right_sibling_id = parent_page->ValueAt(index + 1);
      page = buffer_pool_manager_->FetchPage(right_sibling_id);
      InternalPage *right_sibling_page = reinterpret_cast<InternalPage *>(page);
      page->WLatch();

      Coalesce(&internal_page, &right_sibling_page, &parent_page, index, transaction);

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    return true;
  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(*node);
  if (b_plus_tree_page->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(b_plus_tree_page);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);

    leaf_node->MoveAllTo(neighbor_leaf_node, comparator_);
    buffer_pool_manager_->DeletePage(leaf_node->GetPageId());

    page_id_t parent_id = neighbor_leaf_node->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(page);

    int index = parent_node->ValueIndex(leaf_node->GetPageId());

    parent_node->Remove(index);

    if (parent_node->GetSize() - 1 < parent_node->GetMinSize()) {
      CoalesceOrRedistribute(parent_node, transaction);
    }

    return true;
  }
  else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);

    Page *page = buffer_pool_manager_->FetchPage(internal_node->GetParentPageId());
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(page);

    int index = parent_node->ValueIndex(internal_node->GetPageId());
    KeyType middle_key = parent_node->KeyAt(index);

    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
    buffer_pool_manager_->DeletePage(internal_node->GetPageId());

    parent_node->Remove(index);

    if (parent_node->GetSize() - 1 < parent_node->GetMinSize()) {
      CoalesceOrRedistribute(parent_node, transaction);
    }

    return true;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_page = reinterpret_cast<LeafPage *>(neighbor_node);

    Page *page = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);

    if (index == 0) {
      neighbor_leaf_page->MoveFirstToEndOf(leaf_page, comparator_);

      int i = parent_page->ValueIndex(neighbor_leaf_page->GetPageId());
      parent_page->SetKeyAt(i, neighbor_leaf_page->KeyAt(0));
    }
    else {
      neighbor_leaf_page->MoveLastToFrontOf(leaf_page, comparator_);

      int i = parent_page->ValueIndex(leaf_page->GetPageId());
      parent_page->SetKeyAt(i, leaf_page->KeyAt(0));
    }
  }
  else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_page = reinterpret_cast<InternalPage *>(neighbor_node);
    KeyType middle_key;

    Page *page = buffer_pool_manager_->FetchPage(internal_page->GetParentPageId());
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);

    if (index == 0) {
      Page *page = buffer_pool_manager_->FetchPage(internal_page->GetParentPageId());
      InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
      middle_key = parent_page->KeyAt(parent_page->ValueIndex(neighbor_internal_page->GetPageId()));

      neighbor_internal_page->MoveFirstToEndOf(internal_page, middle_key, buffer_pool_manager_, comparator_);

      int i = parent_page->ValueIndex(neighbor_internal_page->GetPageId());
      parent_page->SetKeyAt(i, neighbor_internal_page->KeyAt(0));
    }
    else {
      neighbor_internal_page->MoveLastToFrontOf(internal_page, middle_key, buffer_pool_manager_, comparator_);

      int i = parent_page->ValueIndex(internal_page->GetPageId());
      parent_page->SetKeyAt(i, internal_page->KeyAt(0));
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    
    return true;
  }
  else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    InternalPage *interbal_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_id = interbal_page->ValueAt(0);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = new_root_id;
    UpdateRootPageId(0);

    Page *page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page);
    b_plus_tree_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);

    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key{}; 
  Page *page = FindLeafPage(key, true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  INDEXITERATOR_TYPE itr(buffer_pool_manager_, leaf_page, 0);
  
  return itr;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);

  int index = leaf_page->KeyIndex(key, comparator_);
  INDEXITERATOR_TYPE itr(buffer_pool_manager_, leaf_page, index);
  return itr;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page);

  while (!b_plus_tree_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    page_id_t right_most = internal_page->ValueAt(internal_page->GetSize() - 1);
    page = buffer_pool_manager_->FetchPage(right_most);
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page);
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(b_plus_tree_page);
  INDEXITERATOR_TYPE itr(buffer_pool_manager_, leaf_page, leaf_page->GetSize());

  return itr;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page); 

  while (!b_plus_tree_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);

    page_id_t page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
    page = buffer_pool_manager_->FetchPage(page_id);
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page);

    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
  
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageRW(const KeyType &key, enum OpType op, bool leftMost, Transaction *transaction) {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page); 

  while (!b_plus_tree_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    
    if (transaction != nullptr) {
      if (op == OpType::READ) {
        page->RLatch();
        UnLatchAndUnpin(op, transaction);
      }
      else {
        page->WLatch();
        if (internal_page->IsSafe(op)) {
          UnLatchAndUnpin(op, transaction);
        }
      }
    }

    if (transaction != nullptr) {
      transaction->AddIntoPageSet(page);
    }

    page_id_t page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
    page = buffer_pool_manager_->FetchPage(page_id);
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page);
  }
  
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLatchAndUnpin(enum OpType op, Transaction *transaction) {
  for (auto *page : *transaction->GetPageSet()){
    if (op == OpType::READ) {
      page->RUnlatch();

      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    else {
      page->WUnlatch();

      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }

}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
