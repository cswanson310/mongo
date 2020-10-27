/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/query/sort_pattern.h"

namespace mongo {
SortPattern::SortPattern(const BSONObj& obj,
                         const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    for (auto&& keyField : obj) {
        auto fieldName = keyField.fieldNameStringData();

        SortPatternPart patternPart;

        if (keyField.type() == Object) {
            BSONObj metaDoc = keyField.Obj();
            // this restriction is due to needing to figure out sort direction
            uassert(17312,
                    "$meta is the only expression supported by $sort right now",
                    metaDoc.firstElement().fieldNameStringData() == "$meta");

            uassert(ErrorCodes::FailedToParse,
                    "Cannot have additional keys in a $meta sort specification",
                    metaDoc.nFields() == 1);

            VariablesParseState vps = pExpCtx->variablesParseState;
            BSONElement metaElem = metaDoc.firstElement();

            if (metaElem.valueStringDataSafe() == "textScore"_sd) {
                // Valid meta sort. Just fall through.
            } else if (metaElem.valueStringDataSafe() == "randVal"_sd) {
                // Valid meta sort. Just fall through.
            } else if (metaElem.valueStringDataSafe() == "searchScore"_sd) {
                uasserted(31218, "$meta sort by 'searchScore' metadata is not supported");
            } else if (metaElem.valueStringDataSafe() == "searchHighlights"_sd) {
                uasserted(31219, "$meta sort by 'searchHighlights' metadata is not supported");
            } else {
                uasserted(31138, str::stream() << "Illegal $meta sort: " << metaElem);
            }
            patternPart.expression = static_cast<ExpressionMeta*>(
                ExpressionMeta::parse(pExpCtx.get(), metaElem, vps).get());

            // If sorting by textScore, sort highest scores first. If sorting by randVal, order
            // doesn't matter, so just always use descending.
            patternPart.direction = SortDirection::kDescending;

            _sortPattern.push_back(std::move(patternPart));
            continue;
        }

        uassert(15974,
                str::stream() << "Illegal key in $sort specification: " << keyField,
                keyField.isNumber());

        const auto direction = keyField.safeNumberLong();

        uassert(15975,
                "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
                ((direction == 1) || (direction == -1)));

        patternPart.fieldPath = FieldPath{fieldName};
        patternPart.direction =
            direction > 0 ? SortDirection::kAscending : SortDirection::kDescending;
        _paths.insert(patternPart.fieldPath->fullPath());
        _sortPattern.push_back(std::move(patternPart));
    }
}

QueryMetadataBitSet SortPattern::metadataDeps(QueryMetadataBitSet unavailableMetadata) const {
    DepsTracker depsTracker{unavailableMetadata};
    for (auto&& part : _sortPattern) {
        if (part.expression) {
            part.expression->addDependencies(&depsTracker);
        }
    }

    return depsTracker.metadataDeps();
}

Document SortPattern::serialize(SortKeySerialization serializationMode) const {
    MutableDocument keyObj;
    const size_t n = _sortPattern.size();
    for (size_t i = 0; i < n; ++i) {
        if (_sortPattern[i].fieldPath) {
            // Append a named integer based on whether the sort is ascending/descending.
            keyObj.setField(_sortPattern[i].fieldPath->fullPath(),
                            Value(_sortPattern[i].direction == SortDirection::kAscending ? 1 : -1));
        } else {
            // Sorting by an expression, use a made up field name.
            auto computedFieldName = std::string(str::stream() << "$computed" << i);
            switch (serializationMode) {
                case SortKeySerialization::kForExplain:
                case SortKeySerialization::kForPipelineSerialization: {
                    const bool isExplain = (serializationMode == SortKeySerialization::kForExplain);
                    keyObj[computedFieldName] = _sortPattern[i].expression->serialize(isExplain);
                    break;
                }
                case SortKeySerialization::kForSortKeyMerging: {
                    // We need to be able to tell which direction the sort is. Expression sorts are
                    // always descending.
                    keyObj[computedFieldName] = Value(-1);
                    break;
                }
            }
        }
    }
    return keyObj.freeze();
}
}  // namespace mongo
