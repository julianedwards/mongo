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

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_ftdc_unwind.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/ftdc/decompressor.h"
#include "mongo/db/ftdc/util.h"

namespace mongo {

using boost::intrusive_ptr;
using std::string;
using std::vector;

/** Helper class to unwind (decompress) ftdc data from a single document. */
class DocumentSourceFTDCUnwind::FTDCUnwinder {
public:
    FTDCUnwinder(const boost::optional<FieldPath>& unwindFTDCPath,
                 bool excludeMetadata,
                 bool excludeMissing);

    /** Reset the unwinder to unwind a new document. */
    void resetDocument(const Document& document);

    // TODO: fix documentation
    /**
     * @return the next document unwound from the document provided to
     * resetDocument(), using the current value in the array located at the
     * provided unwindPath.
     *
     * Returns boost::none if the array is exhausted.
     */
    DocumentSource::GetNextResult getNext();

private:
    // Tracks whether or not we can possibly return any more documents. Note we
    // may return boost::none even if this is true.
    bool _haveNext = false;

    // Path to the ftdc data to unwind.
    const boost::optional<FieldPath> _ftdcUnwindPath;

    // If set, will leave metadata data (ftdc type 0) unmodified.
    const bool _excludeMetadata;

    // If set, will leave out documents without ftdc data from the final
    // result.
    const bool _excludeMissing;

    MutableDocument _output;

    // Document indexes of the field path components.
    vector<Position> _ftdcUnwindPathFieldIndexes;

    // Resulting BSON objects returned from decompression.
    vector<BSONObj> _decompressed;

    // Index into _decompressed to return next.
    size_t _index;

    // Decompressor.
    FTDCDecompressor* _decompressor;
};

DocumentSourceFTDCUnwind::FTDCUnwinder::FTDCUnwinder(
        const boost::optional<FieldPath>& ftdcUnwindPath,
        bool excludeMetadata,
        bool excludeMissing)
    : _ftdcUnwindPath(ftdcUnwindPath),
      _excludeMetadata(excludeMetadata),
      _excludeMissing(excludeMissing),
      _decompressor(new FTDCDecompressor()) {}

void DocumentSourceFTDCUnwind::FTDCUnwinder::resetDocument(const Document& document) {
    // Reset document specific attributes.
    _output.reset(document);
    _ftdcUnwindPathFieldIndexes.clear();
    _decompressed.clear();
    _index = 0;
    _haveNext = true;
}

DocumentSource::GetNextResult DocumentSourceFTDCUnwind::FTDCUnwinder::getNext() {
    if (!_haveNext) {
        return GetNextResult::makeEOF();
    }

    if (_decompressed.empty()) {
        // Get BSONObj.
        BSONObj doc;
        if (_ftdcUnwindPath) {
            auto val = _output.peek().getNestedField(*_ftdcUnwindPath, &_ftdcUnwindPathFieldIndexes);
            if (val.getType() == Object) {
                // TODO: figure out how to turn Value object into BSONObj.
                doc = val.getDocument().toBson();
            }
        } else {
            doc = _output.peek().toBson();
        }

        // check type
        auto swType = FTDCBSONUtil::getBSONDocumentType(doc);
        if (swType.isOK()) {
            // Ignore metadata if specified.
            if (swType.getValue() == FTDCBSONUtil::FTDCType::kMetadata && _excludeMetadata) {
                _haveNext = false;
                return GetNextResult::makeEOF();
            }

            auto swDecompressed = FTDCBSONUtil::getMetricsFromMetricDoc(doc, _decompressor);
            if (!swDecompressed.isOK()) {
                // TODO: figure out what to on error here
                return GetNextResult::makeEOF();
            }
            _decompressed = swDecompressed.getValue();
        } else {
            _haveNext = false;
            // Ignore non-ftdc data if specified.
            if (_excludeMissing) {
                return GetNextResult::makeEOF();
            }
            return _output.freeze();
        }
    }

    auto length = _decompressed.size();
    invariant(_index == 0 || _index < length);

    if (length == 0) {
        _haveNext = false;
        return GetNextResult::makeEOF();
    }

    // Set field to be the next element in the decompressed ftdc vector.
    Document output;
    _haveNext = _index + 1 < length;
    if (_ftdcUnwindPath) {
        _output.setNestedField(_ftdcUnwindPathFieldIndexes, Value(_decompressed[_index]));
        output = _haveNext ? _output.peek() : _output.freeze();
    } else {
        output = Document::fromBsonWithMetaData(_decompressed[_index]);
    }
    _index++;
    return output;
}

DocumentSourceFTDCUnwind::DocumentSourceFTDCUnwind(
        const intrusive_ptr<ExpressionContext>& pExpCtx,
        const boost::optional<FieldPath>& fieldPath,
        bool excludeMetadata,
        bool excludeMissing)
    : DocumentSource(pExpCtx),
      _ftdcUnwindPath(fieldPath),
      _excludeMetadata(excludeMetadata),
      _excludeMissing(excludeMissing),
      _ftdcUnwinder(new FTDCUnwinder(fieldPath, excludeMetadata, excludeMissing)) {}

// TODO: where is this unwind variable coming from?
REGISTER_DOCUMENT_SOURCE(ftdcUnwind,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceFTDCUnwind::createFromBson);

const char* DocumentSourceFTDCUnwind::getSourceName() const {
    return "$ftdcUnwind";
}

intrusive_ptr<DocumentSourceFTDCUnwind> DocumentSourceFTDCUnwind::create(
    const intrusive_ptr<ExpressionContext>& expCtx,
    const boost::optional<string>& ftdcUnwindPath,
    bool excludeMetadata,
    bool excludeMissing) {
    intrusive_ptr<DocumentSourceFTDCUnwind> source(
        new DocumentSourceFTDCUnwind(
            expCtx,
            ftdcUnwindPath ? FieldPath(*ftdcUnwindPath) : boost::optional<FieldPath>(),
            excludeMetadata,
            excludeMissing));
    return source;
}

DocumentSource::GetNextResult DocumentSourceFTDCUnwind::getNext() {
    pExpCtx->checkForInterrupt();

    auto nextOut = _ftdcUnwinder->getNext();
    while (nextOut.isEOF()) {
        // TODO: change this comment
        // No more elements in array currently being unwound. This will loop if the input
        // document is missing the unwind field or has an empty array.
        auto nextInput = pSource->getNext();
        if (!nextInput.isAdvanced()) {
            return nextInput;
        }

        // Try to extract an output document from the new input document.
        _ftdcUnwinder->resetDocument(nextInput.releaseDocument());
        nextOut = _ftdcUnwinder->getNext();
    }

    return nextOut;
}

DocumentSource::GetModPathsReturn DocumentSourceFTDCUnwind::getModifiedPaths() const {
    std::set<std::string> modifiedFields{};
    if (_ftdcUnwindPath) {
        modifiedFields.insert(_ftdcUnwindPath->fullPath());
    }
    return {GetModPathsReturn::Type::kFiniteSet, std::move(modifiedFields), {}};
}

// TODO: figure out what this is for?
Value DocumentSourceFTDCUnwind::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << DOC(
                "path" << (_ftdcUnwindPath ? Value(_ftdcUnwindPath->fullPathWithPrefix()) : Value())
                       << "excludeMetadata"
                       << (_excludeMetadata ? Value(true) : Value())
                       << "excludeMissing"
                       << (_excludeMissing ? Value(true) : Value()))));
}

DepsTracker::State DocumentSourceFTDCUnwind::getDependencies(DepsTracker* deps) const {
    if (_ftdcUnwindPath) {
        deps->fields.insert(_ftdcUnwindPath->fullPath());
    }
    return DepsTracker::State::SEE_NEXT;
}

// TODO: figure out and (probably) change error codes.
intrusive_ptr<DocumentSource> DocumentSourceFTDCUnwind::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    boost::optional<string> prefixedPathString;
    bool excludeMetadata = false;
    bool excludeMissing = false;

    if (elem.type() == Object) {
        for (auto&& subElem : elem.Obj()) {
            if (subElem.fieldNameStringData() == "path") {
                uassert(51242,
                        str::stream() << "expected a string as the path for $ftdcUnwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == String);
                prefixedPathString = subElem.str();
            } else if (subElem.fieldNameStringData() == "excludeMetadata") {
                uassert(51243,
                        str::stream() << "expected a boolean for the excludeMetadata"
                                         "option to $ftdcUnwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Bool);
                excludeMetadata = subElem.Bool();
            } else if (subElem.fieldNameStringData() == "excludeMissing") {
                uassert(51244,
                        str::stream() << "expected a boolean for the excludeMissing"
                                         " option to $unwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Bool);
                excludeMissing = subElem.Bool();
            } else {
                uasserted(51245,
                          str::stream() << "unrecognized option to $ftdcUnwind stage: "
                                        << subElem.fieldNameStringData());
            }
        }
    } else {
        uasserted(
            51246,
            str::stream()
                << "expected an object as specification for $ftdcUnwind stage, got "
                << typeName(elem.type()));
    }

    boost::optional<string> pathString;
    if (prefixedPathString) {
        uassert(51247,
                str::stream() << "path option to $ftdcUnwind stage should be prefixed with a '$': "
                              << prefixedPathString,
                (*prefixedPathString)[0] == '$');
        pathString = Expression::removeFieldPrefix(*prefixedPathString);
    }

    return DocumentSourceFTDCUnwind::create(pExpCtx, pathString, excludeMetadata, excludeMissing);
}
} // namespace mongo
