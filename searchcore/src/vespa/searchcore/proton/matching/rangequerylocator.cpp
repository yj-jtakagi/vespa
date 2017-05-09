// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rangequerylocator.h"

namespace proton::matching {

RangeLimitMetaInfo::RangeLimitMetaInfo() = default;
RangeLimitMetaInfo::RangeLimitMetaInfo(vespalib::stringref low_, vespalib::stringref high_, size_t estimate_)
        : _valid(true),
          _estimate(estimate_),
          _low(low_),
          _high(high_)
{}
RangeLimitMetaInfo::~RangeLimitMetaInfo() {}

}
