// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchlib/btree/btree_key_data.h>
#include <vespa/searchlib/common/bitvector.h>
#include <vespa/searchlib/common/prefilter.h>
#include <vespa/vespalib/util/arrayref.h>

namespace search::attribute {

/*
 * Class providing a synthetic posting list by merging multiple posting lists
 * into an array or bitvector.
 */
template <typename DataT>
class PostingListMerger
{
    using Posting = btree::BTreeKeyData<uint32_t, DataT>;
    using PostingVector = std::vector<Posting>;
    using StartVector = std::vector<size_t>;

    PostingVector              _array;
    StartVector                _startPos;
    std::shared_ptr<BitVector> _bitVector;
    const search::PreFilter   *_preFilter;
    uint32_t                   _docIdLimit;
    bool                       _arrayValid;

    PostingVector &merge(PostingVector &v, PostingVector &temp, const StartVector &startPos) __attribute__((noinline));
public:
    PostingListMerger(uint32_t docIdLimit);

    ~PostingListMerger();

    void setPreFilter(const search::PreFilter *filter) { _preFilter = filter; }
    void reserveArray(uint32_t postingsCount, size_t postingsSize);
    void allocBitVector();
    void merge();
    bool hasArray() const { return _arrayValid; }
    bool hasBitVector() const { return static_cast<bool>(_bitVector); }
    bool emptyArray() const { return _array.empty(); }
    vespalib::ConstArrayRef<Posting> getArray() const { return _array; }
    const BitVector *getBitVector() const { return _bitVector.get(); }
    const std::shared_ptr<BitVector> &getBitVectorSP() const { return _bitVector; }
    uint32_t getDocIdLimit() const { return _docIdLimit; }

    template <typename PostingListType>
    void addToArray(const PostingListType & postingList)
    {
        PostingVector &array = _array;
        if (_preFilter) {
            postingList.foreach([&array, filter=_preFilter](uint32_t key, const DataT &data)
                                { if (filter->keep(key)) { array.emplace_back(key, data); }} );
        } else {
            postingList.foreach([&array](uint32_t key, const DataT &data)
                                { array.emplace_back(key, data); });
        }
        if (_startPos.back() < array.size()) {
            _startPos.push_back(array.size());
        }
    }

    template <typename PostingListType>
    void addToBitVector(const PostingListType & postingList)
    {
        BitVector &bv = *_bitVector;
        uint32_t limit = _docIdLimit;
        if (_preFilter) {
            postingList.foreach_key([&bv, limit, filter=_preFilter](uint32_t key)
                                    { if (filter->keep(key) && __builtin_expect(key < limit, true)) { bv.setBit(key); } });
        } else {
            postingList.foreach_key([&bv, limit](uint32_t key)
                                    { if (__builtin_expect(key < limit, true)) { bv.setBit(key); } });
        }
    }

    // Until diversity handling has been rewritten
    PostingVector &getWritableArray() { return _array; }
    StartVector   &getWritableStartPos() { return _startPos; }
};

}
