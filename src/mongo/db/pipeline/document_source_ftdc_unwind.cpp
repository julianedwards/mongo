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

#include <boost/filesystem/operations.hpp>

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/ftdc/decompressor.h"
#include "mongo/db/ftdc/file_reader.h"
#include "mongo/db/ftdc/ftdc_server.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

using boost::intrusive_ptr;
using std::string;
using std::vector;

/** Helper class to unwind (decompress) ftdc data from a single document. */
class DocumentSourceFTDCUnwind::FTDCUnwinder {
public:
    FTDCUnwinder(const Date_t start, const Date_t end, bool excludeMetadata);

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

    // Start and end for date range.
    const Date_t _start;
    const Date_t _end;

    // If set, will leave metadata data (ftdc type 0) unmodified.
    const bool _excludeMetadata;

    Document _output;

    // Resulting BSON objects returned from decompression.
    vector<BSONObj> _decompressed;

    // Index into _decompressed to return next.
    size_t _index;

    // Decompressor.
    FTDCDecompressor* _decompressor;
};

DocumentSourceFTDCUnwind::FTDCUnwinder::FTDCUnwinder(
        const Date_t start,
        const Date_t end,
        bool excludeMetadata)
    : _start(start),
      _end(end),
      _excludeMetadata(excludeMetadata),
      _decompressor(new FTDCDecompressor()) {}

void DocumentSourceFTDCUnwind::FTDCUnwinder::resetDocument(const Document& document) {
    // Reset document specific attributes.
    _output = document;
    _decompressed.clear();
    _index = 0;
    _haveNext = true;
}

// TODO: refactor this to be less ugly.
DocumentSource::GetNextResult DocumentSourceFTDCUnwind::FTDCUnwinder::getNext() {
    if (!_haveNext) {
        return GetNextResult::makeEOF();
    }

    if (_decompressed.empty()) {
        // Get BSONObj.
        auto doc = _output.toBson();

        // Check type.
        auto swType = FTDCBSONUtil::getBSONDocumentType(doc);
        uassert(51249,
                str::stream() << "invalid ftdc type in diagnostic data",
                swType.isOK());

        // Ignore metadata if specified.
        if (swType.getValue() == FTDCBSONUtil::FTDCType::kMetadata && _excludeMetadata) {
            _haveNext = false;
            return GetNextResult::makeEOF();
        }

        auto swDecompressed = FTDCBSONUtil::getMetricsFromMetricDoc(doc, _decompressor);
        uassert(51251,
                str::stream() << "problem decompressing ftdc from diagnostic data",
                swDecompressed.isOK());
        _decompressed = swDecompressed.getValue();
    }

    auto length = _decompressed.size();
    invariant(_index == 0 || _index < length);
    if (length == 0) {
        _haveNext = false;
        return GetNextResult::makeEOF();
    }

    while (true) {
        if (_index >= length) {
            _haveNext = false;
            return GetNextResult::makeEOF();
        }

        BSONElement element;
        // TODO: expose kFTDCCollectStartField var in src/db/ftdc/util.cpp,
        // this is equal to "start".
        auto status = bsonExtractTypedField(
                _decompressed[_index], "start", BSONType::Date, &element);
        uassert(51252,
            str::stream() << "invalid ftdc id in diagnostic data",
            status.isOK());
        if (element.date() < _start) {
            _index++;
            continue;
        } else if (element.date() >= _end) {
            _haveNext = false;
            return GetNextResult::makeEOF();
        }
        break;
    }

    // Set field to be the next element in the decompressed ftdc vector.
    auto output = Document::fromBsonWithMetaData(_decompressed[_index]);
    _index++;
    _haveNext = _index < length;
    return output;
}

DocumentSourceFTDCUnwind::DocumentSourceFTDCUnwind(
        const intrusive_ptr<ExpressionContext>& pExpCtx,
        const Date_t start,
        const Date_t end,
        bool excludeMetadata)
    : DocumentSource(pExpCtx),
      _excludeMetadata(excludeMetadata),
      _ftdcUnwinder(new FTDCUnwinder(start, end, excludeMetadata)) {
          std::vector<boost::filesystem::path> paths(2);
          auto lower = Date_t::min();
          auto upper = Date_t::max();
          // get ftdc dir
          auto path = getFTDCDirectoryPathParameter();
          if (!path.empty()) {
              // list ftdc files
              boost::filesystem::directory_iterator itEnd;
              for (boost::filesystem::directory_iterator it(path); it != itEnd; it++) {
                  boost::filesystem::path f(*it);
                  if (!boost::filesystem::is_regular_file(f)) {
                      continue;
                  }

                  // parse files (for timestamps)
                  // find files within start and end range
                  auto fn = f.leaf().string();
                  // 34 is the length of all diagnostic data files
                  if (fn.size() != 34) {
                      continue;
                  }

                  // 8 is the length of "metrics."
                  auto dateString = fn.substr(8, 20);
                  dateString[13] = ':';
                  dateString[16] = ':';
                  auto swDate = dateFromISOString(StringData(dateString));
                  if (!swDate.isOK()) {
                      continue;
                  }
                  auto fileDate = swDate.getValue();
                  if (fileDate <= start && fileDate > lower) {
                      lower = fileDate;
                      paths[0] = f;
                  } else if (fileDate >= end && fileDate < upper) {
                      upper = fileDate;
                      paths[1] = f;
                  }
              }
          }

          // TODO: maybe include iterim file here ?

          for (auto path : paths) {
              FTDCFileReader reader(false);
              auto swOpen = reader.open(path);
              uassert(51250,
                      str::stream() << "failed to open diagnostic data file: "
                                    << swOpen.reason(),
                      swOpen.isOK());

              // load those files into bson objs
              while (reader.hasNext().getValue()) {
                  auto out = reader.next();
                  auto obj = std::get<1>(out);
                  auto chunkDate = std::get<2>(out);

                  // find bsonobjs within range
                  if (chunkDate >= start && chunkDate < end) {
                      // add those objs to vector<Document> _compressed
                      _compressed.push_back(Document::fromBsonWithMetaData(obj));
                  }
              }
          }
}

REGISTER_DOCUMENT_SOURCE(ftdcUnwind,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceFTDCUnwind::createFromBson);

const char* DocumentSourceFTDCUnwind::getSourceName() const {
    return "$ftdcUnwind";
}

intrusive_ptr<DocumentSourceFTDCUnwind> DocumentSourceFTDCUnwind::create(
    const intrusive_ptr<ExpressionContext>& expCtx,
    const Date_t start,
    const Date_t end,
    bool excludeMetadata) {
    intrusive_ptr<DocumentSourceFTDCUnwind> source(
            new DocumentSourceFTDCUnwind(expCtx, start, end, excludeMetadata));

    return source;
}

DocumentSource::GetNextResult DocumentSourceFTDCUnwind::getNext() {
    pExpCtx->checkForInterrupt();

    auto nextOut = _ftdcUnwinder->getNext();
    if (nextOut.isEOF() && _cIndex < _compressed.size()) {
        // Try to extract an output document from the new input document.
        _ftdcUnwinder->resetDocument(_compressed[_cIndex]);
        nextOut = _ftdcUnwinder->getNext();
        _cIndex++;
    }

    return nextOut;
}

Value DocumentSourceFTDCUnwind::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << DOC(
                    "excludeMetadata" << (_excludeMetadata ? Value(true) : Value()))));
}

DepsTracker::State DocumentSourceFTDCUnwind::getDependencies(DepsTracker* deps) const {
    return DepsTracker::State::SEE_NEXT;
}

// TODO: figure out and (probably) change error codes.
intrusive_ptr<DocumentSource> DocumentSourceFTDCUnwind::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    Date_t start;
    Date_t end;
    bool excludeMetadata = false;

    if (elem.type() == Object) {
        for (auto&& subElem : elem.Obj()) {
            if (subElem.fieldNameStringData() == "start") {
                uassert(51242,
                        str::stream() << "expected a date as start time for $ftdcUnwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Date);
                start = subElem.date();
            } else if (subElem.fieldNameStringData() == "end") {
                uassert(51243,
                        str::stream() << "expected a date as end time for $ftdcUnwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Date);
                end = subElem.date();
            } else if (subElem.fieldNameStringData() == "excludeMetadata") {
                uassert(51244,
                        str::stream() << "expected a boolean for the excludeMetadata"
                                         "option to $ftdcUnwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Bool);
                excludeMetadata = subElem.Bool();
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

    auto diff = end - start;
    uassert(51247,
            str::stream() << "must specify 0 > end - start <= 60 min",
            diff > Milliseconds(0) && diff <= Minutes(60));

    return DocumentSourceFTDCUnwind::create(pExpCtx, start, end, excludeMetadata);
}
} // namespace mongo
