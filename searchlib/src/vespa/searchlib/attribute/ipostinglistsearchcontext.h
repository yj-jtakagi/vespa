// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <memory>

namespace search::queryeval { class SearchIterator; }
namespace search::fef { class TermFieldMatchData; }
namespace search { class PreFilter; }

namespace search::attribute {


/**
 * Interface for search context helper classes to create attribute
 * search iterators based on posting lists and using dictionary
 * information to better estimate number of hits.  Also used for
 * enumerated attributes without posting lists to eliminate brute
 * force searches for nonexisting values.
 */

class IPostingListSearchContext
{
protected:
    IPostingListSearchContext() { }
    virtual ~IPostingListSearchContext() { }

public:
    /**
     * Will load/prepare any postings lists. Will take strictness and optional filter into account.
     * @param strict If true iterator must advance to next valid docid.
     * @param filter Any prefilter that can be applied to posting lists for optimization purposes.
     */
    virtual void fetchPostings(bool strict, const PreFilter * filter) = 0;
    virtual std::unique_ptr<queryeval::SearchIterator> createPostingIterator(fef::TermFieldMatchData *matchData, bool strict) = 0;
    virtual unsigned int approximateHits() const = 0;
};

}
