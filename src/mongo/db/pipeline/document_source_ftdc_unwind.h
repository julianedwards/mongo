/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/field_path.h"

namespace mongo {

class DocumentSourceFTDCUnwind final : public DocumentSource {
public:
    // Virtuals from DocumentSource.
    GetNextResult getNext() final;

    const char* getSourceName() const final;

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    // TODO: figure out how this will work if the whole doc is being ftdc unwound
    /**
     * Returns the unwound path.
     */
    GetModPathsReturn getModifiedPaths() const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kAllowed,
                                     TransactionRequirement::kAllowed,
                                     LookupRequirement::kAllowed);

        constraints.canSwapWithMatch = true;
        return constraints;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    DepsTracker::State getDependencies(DepsTracker* deps) const final;

    /**
     * Creates a new $ftdcUnwind DocumentSource from a BSON specification.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    static boost::intrusive_ptr<DocumentSourceFTDCUnwind> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const boost::optional<std::string>& path,
        bool excludeMetadata,
        bool excludeMissing);

    std::string getFTDCUnwindPath() const {
        return _ftdcUnwindPath->fullPath();
    }

private:
    DocumentSourceFTDCUnwind(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                             const boost::optional<FieldPath>& fieldPath,
                             bool excludeMetadata,
                             bool excludeMissing);

    // Configuration state.
    const boost::optional<FieldPath> _ftdcUnwindPath;
    // If set, the $ftdcUnwind stage will exclude metadata documents (ftdc type
    // 0 documents).
    const bool _excludeMetadata;
    // If set, will exclude documents that do not include ftdc data from this
    // stage's output.
    const bool _excludeMissing;

    // Iteration state.
    class FTDCUnwinder;
    std::unique_ptr<FTDCUnwinder> _ftdcUnwinder;
};

}  // namespace mongo
