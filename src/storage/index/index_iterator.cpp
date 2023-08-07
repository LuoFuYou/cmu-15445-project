/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *leaf_page, int index):
                    buffer_pool_manager_(buffer_pool_manager),
                    leaf_page_(leaf_page),
                    index_(index) {};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
};

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { 
    if (leaf_page_->GetNextPageId() == 0 && index_ == leaf_page_->GetSize()) {
        return true;
    }

    return false; 
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    if (++index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() != 0) {
        Page *page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
        leaf_page_ = reinterpret_cast<LeafPage *>(page);
        index_ = 0;
    }

    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) {
    if (this->leaf_page_->GetPageId() == itr.leaf_page_->GetPageId() && this->index_ == itr.index_) {
        return true;
    }

    return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) {
    return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
