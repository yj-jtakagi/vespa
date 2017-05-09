// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchlib/queryeval/searchable.h>
#include <vespa/vespalib/stllike/string.h>
#include <vespa/searchlib/queryeval/searchiterator.h>
#include <vespa/searchlib/queryeval/blueprint.h>

#include <vespa/searchlib/fef/matchdata.h>
#include <mutex>

namespace proton::matching {

class RangeLimitMetaInfo {
public:
    RangeLimitMetaInfo();
    RangeLimitMetaInfo(vespalib::stringref low, vespalib::stringref high, size_t estimate);
    ~RangeLimitMetaInfo();
    const vespalib::string & low() const { return _low; }
    const vespalib::string & high() const { return _high; }
    bool valid() const { return _valid; }
    size_t estimate() const { return _estimate; }
private:
    bool _valid;
    size_t _estimate;
    vespalib::string _low;
    vespalib::string _high;
};

class RangeQueryLocator {
public:
    virtual ~RangeQueryLocator() {}
    virtual RangeLimitMetaInfo locate(vespalib::stringref field) const = 0;
};

/**
 * This class is responsible for creating attribute-based search
 * iterators that are used to limit the search space. Each search
 * thread wants a separate search iterator, but the blueprint is
 * shared between threads. All threads should request the same number
 * of hits, so this class just lets the first thread requesting a
 * search decide the number of hits in the underlying blueprint.
 **/
class AttributeLimiter
{
public:
    enum DiversityCutoffStrategy { LOOSE, STRICT};
    AttributeLimiter(const RangeQueryLocator & _rangeQueryLocator,
                     search::queryeval::Searchable &searchable_attributes,
                     const search::queryeval::IRequestContext & requestContext,
                     const vespalib::string &attribute_name, bool descending,
                     const vespalib::string &diversity_attribute,
                     double diversityCutoffFactor,
                     DiversityCutoffStrategy diversityCutoffStrategy);
    ~AttributeLimiter();
    search::queryeval::SearchIterator::UP create_search(size_t want_hits, size_t max_group_size, bool strictSearch);
    bool was_used() const { return ((!_match_datas.empty()) || (_blueprint.get() != nullptr)); }
    ssize_t getEstimatedHits() const { return _estimatedHits; }
    static DiversityCutoffStrategy toDiversityCutoffStrategy(const vespalib::stringref & strategy);
private:
    const vespalib::string & toString(DiversityCutoffStrategy strategy);
    search::queryeval::Searchable            & _searchable_attributes;
    const search::queryeval::IRequestContext & _requestContext;
    const RangeQueryLocator                  & _rangeQueryLocator;
    vespalib::string                           _attribute_name;
    bool                                       _descending;
    vespalib::string                           _diversity_attribute;
    std::mutex                                 _lock;
    std::vector<search::fef::MatchData::UP>    _match_datas;
    search::queryeval::Blueprint::UP           _blueprint;
    ssize_t                                    _estimatedHits;
    double                                     _diversityCutoffFactor;
    DiversityCutoffStrategy                    _diversityCutoffStrategy;
};

}
