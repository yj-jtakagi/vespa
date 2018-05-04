// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchcommon/common/range.h>
#include <vespa/vespalib/stllike/string.h>


namespace search::fef { class TermFieldMatchData; }
namespace search::queryeval { class SearchIterator; }
namespace search {
    class QueryTermBase;
    class BitVector;
}

namespace search::attribute {

class ISearchContext {
public:
    using UP = std::unique_ptr<ISearchContext>;
    using DocId = uint32_t;

private:
    virtual bool onCmp(DocId docId, int32_t &weight) const = 0;
    virtual bool onCmp(DocId docId) const = 0;

public:
    virtual ~ISearchContext() {}

    virtual unsigned int approximateHits() const = 0;

    /**
     * Creates an attribute search iterator associated with this
     * search context.
     *
     * @return attribute search iterator
     *
     * @param matchData the attribute match data used when
     * unpacking data for a hit
     *
     * @param strict whether the iterator should be strict or not
     **/
    virtual std::unique_ptr<queryeval::SearchIterator>
    createIterator(fef::TermFieldMatchData *matchData, bool strict) = 0;

    /*
     * Create temporary posting lists.
     * Should be called before createIterator() is called.
     * Will load/prepare any postings lists. Will take strictness and optional filter into account.
     * @param strict If true iterator must advance to next valid docid.
     * @param filter Any prefilter that can be applied to posting lists for optimization purposes.
     */
    virtual void fetchPostings(bool strict, const BitVector * filter) = 0;

    virtual bool valid() const = 0;
    virtual Int64Range getAsIntegerTerm() const = 0;
    virtual const QueryTermBase &queryTerm() const = 0;
    virtual const vespalib::string &attributeName() const = 0;

    bool cmp(DocId docId, int32_t &weight) const { return onCmp(docId, weight); }
    bool cmp(DocId docId) const { return onCmp(docId); }

};

}
