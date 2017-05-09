// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rangequerylocator.h"
#include <vespa/searchlib/queryeval/intermediate_blueprints.h>

using namespace search::queryeval;

namespace proton::matching {

RangeLimitMetaInfo::RangeLimitMetaInfo() = default;
RangeLimitMetaInfo::RangeLimitMetaInfo(vespalib::stringref low_, vespalib::stringref high_, size_t estimate_)
    : _valid(true),
      _estimate(estimate_),
      _low(low_),
      _high(high_)
{}
RangeLimitMetaInfo::~RangeLimitMetaInfo() {}

namespace {

RangeLimitMetaInfo
locateFirst(vespalib::stringref field, const Blueprint & blueprint) {
    if (blueprint.isIntermediate()) {
        const IntermediateBlueprint & intermediate = static_cast<const IntermediateBlueprint &>(blueprint);
        if (dynamic_cast<const AndNotBlueprint *>(&intermediate) != nullptr) {
            return locateFirst(field, intermediate.getChild(0));
        } else if (dynamic_cast<const RankBlueprint *>(&intermediate) != nullptr) {
            return locateFirst(field, intermediate.getChild(0));
        } else if (dynamic_cast<const AndBlueprint *>(&intermediate) != nullptr) {
            for (size_t i(0); i < intermediate.childCnt(); i++) {
                RangeLimitMetaInfo childMeta = locateFirst(field, intermediate.getChild(i));
                if (childMeta.valid()) {
                    return childMeta;
                }
            }
        }
    } else {
        const Blueprint::State & state = blueprint.getState();
        // TODO: Find fieldId
        if (state.isTermLike() && (state.numFields() == 1) && (state.field(0).getFieldId() == 7)) {
            const LeafBlueprint &leaf = static_cast<const LeafBlueprint &>(blueprint);
            vespalib::string from;
            vespalib::string too;
            if (leaf.getRange(from, too)) {
                return RangeLimitMetaInfo(from, too, state.estimate().estHits);
            }
        }
    }
    return RangeLimitMetaInfo();
}

}

RangeLimitMetaInfo
LocateRangeItemFromQuery::locate(vespalib::stringref field) const {
    return locateFirst(field, _blueprint);
}

}

