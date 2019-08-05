// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rawfieldvalue.h"
#include "literalfieldvalue.hpp"
#include <vespa/document/util/stringutil.h>
#include <vespa/vespalib/util/xmlstream.h>

using namespace vespalib::xml;

namespace document {

IMPLEMENT_IDENTIFIABLE(RawFieldValue, LiteralFieldValueB);

void
RawFieldValue::printXml(XmlOutputStream& out) const
{
    out << XmlBase64Content()
        << XmlContentWrapper(_value.data(), _value.size());
}

void
RawFieldValue::print(std::ostream& out, bool, const std::string&) const
{
    StringUtil::printAsHex(out, _value.data(), _value.size());
}

template class LiteralFieldValue<RawFieldValue, DataType::T_RAW, false>;
} // document
