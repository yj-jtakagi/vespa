// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "noaggregated.h"
#include "minmaxaggregated.h"
#include "btree_key_data.h"
#include <vespa/vespalib/datastore/entryref.h>
#include <vespa/vespalib/datastore/handle.h>
#include <cassert>
#include <utility>
#include <cstddef>

namespace search::datastore {

template <typename, typename> class Allocator;
template <typename> class BufferType;

namespace allocator {
template <typename, typename ...> struct Assigner;
}

}

namespace search::btree {

template <typename, typename, typename, size_t, size_t> class BTreeNodeAllocator;
template <typename, typename, typename, size_t, size_t> class BTreeNodeStore;

class NoAggregated;

class BTreeNode {
private:
    uint8_t _level;
    bool _isFrozen;
public:
    static constexpr uint8_t EMPTY_LEVEL = 255;
    static constexpr uint8_t LEAF_LEVEL = 0;
protected:
    uint16_t _validSlots;
    BTreeNode(uint8_t level)
        : _level(level),
          _isFrozen(false),
          _validSlots(0)
    {}

    BTreeNode(const BTreeNode &rhs)
        : _level(rhs._level),
          _isFrozen(rhs._isFrozen),
          _validSlots(rhs._validSlots)
    {}

    BTreeNode &
    operator=(const BTreeNode &rhs)
    {
        assert(!_isFrozen);
        _level = rhs._level;
        _isFrozen = rhs._isFrozen;
        _validSlots = rhs._validSlots;
        return *this;
    }

    ~BTreeNode() { assert(_isFrozen); }

public:
    typedef datastore::EntryRef Ref;

    bool isLeaf() const { return _level == 0u; }
    bool getFrozen() const { return _isFrozen; }
    void freeze() { _isFrozen = true; }
    void unFreeze() { _isFrozen = false; }
    void setLevel(uint8_t level) { _level = level; }
    uint32_t getLevel() const { return _level; }
    uint32_t validSlots() const { return _validSlots; }
    void setValidSlots(uint16_t validSlots_) { _validSlots = validSlots_; }
};


/**
 * Use of BTreeNoLeafData class triggers the below partial
 * specialization of BTreeNodeDataWrap to prevent unneeded storage
 * overhead.
 */
template <class DataT, uint32_t NumSlots>
class BTreeNodeDataWrap
{
public:
    DataT _data[NumSlots];

    BTreeNodeDataWrap() : _data() {}
    ~BTreeNodeDataWrap() { }

    void copyData(const BTreeNodeDataWrap &rhs, uint32_t validSlots) {
        const DataT *rdata = rhs._data;
        DataT *ldata = _data;
        DataT *ldatae = _data + validSlots;
        for (; ldata != ldatae; ++ldata, ++rdata)
            *ldata = *rdata;
    }

    const DataT &getData(uint32_t idx) const { return _data[idx]; }
    void setData(uint32_t idx, const DataT &data) { _data[idx] = data; }
    static bool hasData() { return true; }
};


template <uint32_t NumSlots>
class BTreeNodeDataWrap<BTreeNoLeafData, NumSlots>
{
public:
    BTreeNodeDataWrap() {}

    void copyData(const BTreeNodeDataWrap &rhs, uint32_t validSlots) {
        (void) rhs;
        (void) validSlots;
    }

    const BTreeNoLeafData &getData(uint32_t idx) const {
        (void) idx;
        return BTreeNoLeafData::_instance;
    }

    void setData(uint32_t idx, const BTreeNoLeafData &data) {
        (void) idx;
        (void) data;
    }

    static bool hasData() { return false; }
};


template <typename AggrT>
class BTreeNodeAggregatedWrap
{
    typedef AggrT AggregatedType;

    AggrT _aggr;
    static AggrT _instance;

public:
    BTreeNodeAggregatedWrap()
        : _aggr()
    {}
    AggrT &getAggregated() { return _aggr; }
    const AggrT &getAggregated() const { return _aggr; }
    static const AggrT &getEmptyAggregated() { return _instance; }
};

template <typename AggrT>
AggrT BTreeNodeAggregatedWrap<AggrT>::_instance;

template <>
class BTreeNodeAggregatedWrap<NoAggregated>
{
    typedef NoAggregated AggregatedType;

    static NoAggregated _instance;
public:
    BTreeNodeAggregatedWrap() {}

    NoAggregated &getAggregated() { return _instance; }
    const NoAggregated &getAggregated() const { return _instance; }
    static const NoAggregated &getEmptyAggregated() { return _instance; }
};

template <> MinMaxAggregated BTreeNodeAggregatedWrap<MinMaxAggregated>::_instance;

template <typename KeyT, uint32_t NumSlots>
class BTreeNodeT : public BTreeNode {
protected:
    KeyT _keys[NumSlots];
    BTreeNodeT(uint8_t level)
        : BTreeNode(level),
          _keys()
    {}

    ~BTreeNodeT() {}

    BTreeNodeT(const BTreeNodeT &rhs)
        : BTreeNode(rhs)
    {
        const KeyT *rkeys = rhs._keys;
        KeyT *lkeys = _keys;
        KeyT *lkeyse = _keys + _validSlots;
        for (; lkeys != lkeyse; ++lkeys, ++rkeys)
            *lkeys = *rkeys;
    }

    BTreeNodeT &
    operator=(const BTreeNodeT &rhs)
    {
        BTreeNode::operator=(rhs);
        const KeyT *rkeys = rhs._keys;
        KeyT *lkeys = _keys;
        KeyT *lkeyse = _keys + _validSlots;
        for (; lkeys != lkeyse; ++lkeys, ++rkeys)
            *lkeys = *rkeys;
        return *this;
    }

public:
    const KeyT & getKey(uint32_t idx) const { return _keys[idx]; }
    const KeyT & getLastKey() const { return _keys[validSlots() - 1]; }
    void writeKey(uint32_t idx, const KeyT & key) { _keys[idx] = key; }

    template <typename CompareT>
    uint32_t lower_bound(uint32_t sidx, const KeyT & key, CompareT comp) const;

    template <typename CompareT>
    uint32_t lower_bound(const KeyT & key, CompareT comp) const;

    template <typename CompareT>
    uint32_t upper_bound(uint32_t sidx, const KeyT & key, CompareT comp) const;

    bool isFull() const { return validSlots() == NumSlots; }
    bool isAtLeastHalfFull() const { return validSlots() >= minSlots(); }
    static uint32_t maxSlots() { return NumSlots; }
    static uint32_t minSlots() { return NumSlots / 2; }
};

template <typename KeyT, typename DataT, typename AggrT, uint32_t NumSlots>
class BTreeNodeTT : public BTreeNodeT<KeyT, NumSlots>,
                    public BTreeNodeDataWrap<DataT, NumSlots>,
                    public BTreeNodeAggregatedWrap<AggrT>
{
public:
    typedef BTreeNodeT<KeyT, NumSlots> ParentType;
    typedef BTreeNodeDataWrap<DataT, NumSlots> DataWrapType;
    typedef BTreeNodeAggregatedWrap<AggrT> AggrWrapType;
    using ParentType::_validSlots;
    using ParentType::validSlots;
    using ParentType::getFrozen;
    using ParentType::_keys;
    using DataWrapType::getData;
    using DataWrapType::setData;
    using DataWrapType::copyData;
protected:
    BTreeNodeTT(uint8_t level)
        : ParentType(level),
          DataWrapType()
    {}

    ~BTreeNodeTT() {}

    BTreeNodeTT(const BTreeNodeTT &rhs)
        : ParentType(rhs),
          DataWrapType(rhs),
          AggrWrapType(rhs)
    {
        copyData(rhs, _validSlots);
    }

    BTreeNodeTT &operator=(const BTreeNodeTT &rhs) {
        ParentType::operator=(rhs);
        AggrWrapType::operator=(rhs);
        copyData(rhs, _validSlots);
        return *this;
    }

public:
    typedef BTreeNodeTT<KeyT, DataT, AggrT, NumSlots> NodeType;
    void insert(uint32_t idx, const KeyT & key, const DataT & data);
    void update(uint32_t idx, const KeyT & key, const DataT & data) {
        // assert(idx < NodeType::maxSlots());
        // assert(!getFrozen());
        _keys[idx] = key;
        setData(idx, data);
    }
    void splitInsert(NodeType * splitNode, uint32_t idx, const KeyT & key, const DataT & data);
    void remove(uint32_t idx);
    void stealAllFromLeftNode(const NodeType * victim);
    void stealAllFromRightNode(const NodeType * victim);
    void stealSomeFromLeftNode(NodeType * victim);
    void stealSomeFromRightNode(NodeType * victim);
    void cleanRange(uint32_t from, uint32_t to);
    void clean();
    void cleanFrozen();
};

template <typename KeyT, typename AggrT, uint32_t NumSlots = 16>
class BTreeInternalNode : public BTreeNodeTT<KeyT, BTreeNode::Ref, AggrT,
                                             NumSlots>
{
public:
    typedef BTreeNodeTT<KeyT, BTreeNode::Ref, AggrT, NumSlots> ParentType;
    typedef BTreeInternalNode<KeyT, AggrT, NumSlots> InternalNodeType;
    template <typename, typename, typename, size_t, size_t>
    friend class BTreeNodeAllocator;
    template <typename, typename, typename, size_t, size_t>
    friend class BTreeNodeStore;
    template <typename, uint32_t>
    friend class BTreeNodeDataWrap;
    template <typename>
    friend class datastore::BufferType;
    template <typename, typename>
    friend class datastore::Allocator;
    template <typename, typename...>
    friend struct datastore::allocator::Assigner;
    typedef BTreeNode::Ref Ref;
    typedef datastore::Handle<InternalNodeType> RefPair;
    using ParentType::_keys;
    using ParentType::validSlots;
    using ParentType::_validSlots;
    using ParentType::getFrozen;
    using ParentType::getData;
    using ParentType::setData;
    using ParentType::setLevel;
    using ParentType::EMPTY_LEVEL;
    typedef KeyT KeyType;
    typedef Ref DataType;
private:
    uint32_t _validLeaves;

    BTreeInternalNode()
        : ParentType(EMPTY_LEVEL),
          _validLeaves(0u)
    {}

    BTreeInternalNode(const BTreeInternalNode &rhs)
        : ParentType(rhs),
          _validLeaves(rhs._validLeaves)
    {}

    ~BTreeInternalNode() {}

    BTreeInternalNode &operator=(const BTreeInternalNode &rhs) {
        ParentType::operator=(rhs);
        _validLeaves = rhs._validLeaves;
        return *this;
    }

    template <typename NodeAllocatorType>
    uint32_t countValidLeaves(uint32_t start, uint32_t end, NodeAllocatorType &allocator);

public:
    BTreeNode::Ref getChild(uint32_t idx) const { return getData(idx); }
    void setChild(uint32_t idx, BTreeNode::Ref child) { setData(idx, child); }
    BTreeNode::Ref getLastChild() const { return getChild(validSlots() - 1); }
    uint32_t validLeaves() const { return _validLeaves; }
    void setValidLeaves(uint32_t newValidLeaves) { _validLeaves = newValidLeaves; }
    void incValidLeaves(uint32_t delta) { _validLeaves += delta; }
    void decValidLeaves(uint32_t delta) { _validLeaves -= delta; }

    template <typename NodeAllocatorType>
    void splitInsert(BTreeInternalNode *splitNode, uint32_t idx, const KeyT &key,
                     const BTreeNode::Ref &data, NodeAllocatorType &allocator);

    void stealAllFromLeftNode(const BTreeInternalNode *victim);
    void stealAllFromRightNode(const BTreeInternalNode *victim);

    template <typename NodeAllocatorType>
    void stealSomeFromLeftNode(BTreeInternalNode *victim, NodeAllocatorType &allocator);

    template <typename NodeAllocatorType>
    void stealSomeFromRightNode(BTreeInternalNode *victim, NodeAllocatorType &allocator);

    void clean();
    void cleanFrozen();

    template <typename NodeStoreType, typename FunctionType>
    void foreach_key(NodeStoreType &store, FunctionType func) const {
        const BTreeNode::Ref *it = this->_data;
        const BTreeNode::Ref *ite = it + _validSlots;
        if (this->getLevel() > 1u) {
            for (; it != ite; ++it) {
                store.mapInternalRef(*it)->foreach_key(store, func);
            }
        } else {
            for (; it != ite; ++it) {
                store.mapLeafRef(*it)->foreach_key(func);
            }
        }
    }

    template <typename NodeStoreType, typename FunctionType>
    void foreach(NodeStoreType &store, FunctionType func) const {
        const BTreeNode::Ref *it = this->_data;
        const BTreeNode::Ref *ite = it + _validSlots;
        if (this->getLevel() > 1u) {
            for (; it != ite; ++it) {
                store.mapInternalRef(*it)->foreach(store, func);
            }
        } else {
            for (; it != ite; ++it) {
                store.mapLeafRef(*it)->foreach(func);
            }
        }
    }
};

template <typename KeyT, typename DataT, typename AggrT,
          uint32_t NumSlots = 16>
class BTreeLeafNode : public BTreeNodeTT<KeyT, DataT, AggrT, NumSlots>
{
public:
    typedef BTreeNodeTT<KeyT, DataT, AggrT, NumSlots> ParentType;
    typedef BTreeLeafNode<KeyT, DataT, AggrT, NumSlots> LeafNodeType;
    template <typename, typename, typename, size_t, size_t>
    friend class BTreeNodeAllocator;
    template <typename, typename, typename, size_t, size_t>
    friend class BTreeNodeStore;
    template <typename>
    friend class datastore::BufferType;
    template <typename, typename>
    friend class datastore::Allocator;
    template <typename, typename...>
    friend struct datastore::allocator::Assigner;
    typedef BTreeNode::Ref Ref;
    typedef datastore::Handle<LeafNodeType> RefPair;
    using ParentType::validSlots;
    using ParentType::_validSlots;
    using ParentType::_keys;
    using ParentType::freeze;
    using ParentType::stealSomeFromLeftNode;
    using ParentType::stealSomeFromRightNode;
    using ParentType::LEAF_LEVEL;
    typedef BTreeKeyData<KeyT, DataT> KeyDataType;
    typedef KeyT KeyType;
    typedef DataT DataType;
private:
    BTreeLeafNode() : ParentType(LEAF_LEVEL) {}

protected:
    BTreeLeafNode(const BTreeLeafNode &rhs)
        : ParentType(rhs)
    {}

    BTreeLeafNode(const KeyDataType *smallArray, uint32_t arraySize);

    ~BTreeLeafNode() {}

    BTreeLeafNode &operator=(const BTreeLeafNode &rhs) {
        ParentType::operator=(rhs);
        return *this;
    }

public:
    template <typename NodeAllocatorType>
    void stealSomeFromLeftNode(BTreeLeafNode *victim, NodeAllocatorType &allocator)
    {
        (void) allocator;
        stealSomeFromLeftNode(victim);
    }

    template <typename NodeAllocatorType>
    void stealSomeFromRightNode(BTreeLeafNode *victim, NodeAllocatorType &allocator) {
        (void) allocator;
        stealSomeFromRightNode(victim);
    }

    const DataT &getLastData() const { return this->getData(validSlots() - 1); }
    void writeData(uint32_t idx, const DataT &data) { this->setData(idx, data); }
    uint32_t validLeaves() const { return validSlots(); }

    template <typename FunctionType>
    void foreach_key(FunctionType func) const {
        const KeyT *it = _keys;
        const KeyT *ite = it + _validSlots;
        for (; it != ite; ++it) {
            func(*it);
        }
    }

    template <typename FunctionType>
    void foreach(FunctionType func) const {
        const KeyT *it = _keys;
        const KeyT *ite = it + _validSlots;
        uint32_t idx = 0;
        for (; it != ite; ++it) {
            func(*it, this->getData(idx++));
        }
    }
};


template <typename KeyT, typename DataT, typename AggrT,
          uint32_t NumSlots = 16>
class BTreeLeafNodeTemp : public BTreeLeafNode<KeyT, DataT, AggrT, NumSlots>
{
public:
    typedef BTreeLeafNode<KeyT, DataT, AggrT, NumSlots> ParentType;
    typedef typename ParentType::KeyDataType KeyDataType;

    BTreeLeafNodeTemp(const KeyDataType *smallArray,
                      uint32_t arraySize)
        : ParentType(smallArray, arraySize)
    {}

    ~BTreeLeafNodeTemp() {}
};

extern template class BTreeNodeDataWrap<uint32_t, 16>;
extern template class BTreeNodeDataWrap<BTreeNoLeafData, 16>;
extern template class BTreeNodeT<uint32_t, 16>;
extern template class BTreeNodeTT<uint32_t, uint32_t, NoAggregated, 16>;
extern template class BTreeNodeTT<uint32_t, BTreeNoLeafData, NoAggregated, 16>;
extern template class BTreeNodeTT<uint32_t, datastore::EntryRef, NoAggregated, 16>;
extern template class BTreeNodeTT<uint32_t, int32_t, MinMaxAggregated, 16>;
extern template class BTreeInternalNode<uint32_t, NoAggregated, 16>;
extern template class BTreeInternalNode<uint32_t, MinMaxAggregated, 16>;
extern template class BTreeLeafNode<uint32_t, uint32_t, NoAggregated, 16>;
extern template class BTreeLeafNode<uint32_t, BTreeNoLeafData, NoAggregated,
                                    16>;
extern template class BTreeLeafNode<uint32_t, int32_t, MinMaxAggregated, 16>;
extern template class BTreeLeafNodeTemp<uint32_t, uint32_t, NoAggregated, 16>;
extern template class BTreeLeafNodeTemp<uint32_t, int32_t, MinMaxAggregated,
                                        16>;
extern template class BTreeLeafNodeTemp<uint32_t, BTreeNoLeafData,
                                        NoAggregated, 16>;

}
