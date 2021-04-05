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

#pragma once

#include <boost/iostreams/device/mapped_file.hpp>

#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/values/bson.h"

namespace mongo {
namespace sbe {
class BSONScanStage final : public PlanStage {
public:
    BSONScanStage(const char* bsonBegin,
                  const char* bsonEnd,
                  boost::optional<value::SlotId> recordSlot,
                  std::vector<std::string> fields,
                  value::SlotVector vars,
                  PlanNodeId planNodeId);

    BSONScanStage(std::string fileName,
                  boost::optional<value::SlotId> recordSlot,
                  std::vector<std::string> fields,
                  value::SlotVector vars,
                  PlanNodeId planNodeId);

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats(bool includeDebugInfo) const final;
    const SpecificStats* getSpecificStats() const final;

    std::vector<DebugPrinter::Block> debugPrint() const final;

private:
    // This is not an ideal place to store the bson bytes here but it will do for now.
    const std::string _fileName;
    boost::iostreams::mapped_file_source _mappedFile;

    const char* _bsonBegin;
    const char* _bsonEnd;

    const boost::optional<value::SlotId> _recordSlot;
    const std::vector<std::string> _fields;
    const value::SlotVector _vars;

    std::unique_ptr<value::ViewOfValueAccessor> _recordAccessor;

    value::FieldViewAccessorMap _fieldAccessors;
    value::SlotAccessorMap _varAccessors;

    const char* _bsonCurrent;

    ScanStats _specificStats;
};

class ParallelBsonScanStage final : public PlanStage {
    struct BsonRange {
        const char* begin;
        const char* end;
        size_t count;
    };

    struct ParallelState {
        Mutex mutex = MONGO_MAKE_LATCH("ParallelBsonScanStage::ParallelState::mutex");

        // This is not an ideal place to store the bson bytes here but it will do for now.
        const std::string fileName;
        boost::iostreams::mapped_file_source mappedFile;

        const char* bsonBegin{nullptr};
        const char* bsonEnd{nullptr};

        std::vector<BsonRange> ranges;
        AtomicWord<size_t> currentRange{0};

        ParallelState(const char* begin, const char* end) : bsonBegin(begin), bsonEnd(end) {}
        ParallelState(const std::string& name) : fileName(name) {}
    };

public:
    ParallelBsonScanStage(const char* bsonBegin,
                          const char* bsonEnd,
                          boost::optional<value::SlotId> recordSlot,
                          std::vector<std::string> fields,
                          value::SlotVector vars,
                          PlanNodeId planNodeId);

    ParallelBsonScanStage(std::string fileName,
                          boost::optional<value::SlotId> recordSlot,
                          std::vector<std::string> fields,
                          value::SlotVector vars,
                          PlanNodeId planNodeId);

    ParallelBsonScanStage(const std::shared_ptr<ParallelState>& state,
                          boost::optional<value::SlotId> recordSlot,
                          std::vector<std::string> fields,
                          value::SlotVector vars,
                          PlanNodeId planNodeId);

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats(bool /*includeDebugInfo*/) const final;
    const SpecificStats* getSpecificStats() const final;

    std::vector<DebugPrinter::Block> debugPrint() const final;

private:
    void nextRange() {
        _currentRange = _state->currentRange.fetchAndAdd(1);
        if (_currentRange < _state->ranges.size()) {
            _range = _state->ranges[_currentRange];
        }
    }

    std::shared_ptr<ParallelState> _state;

    const boost::optional<value::SlotId> _recordSlot;
    const std::vector<std::string> _fields;
    const value::SlotVector _vars;

    std::unique_ptr<value::ViewOfValueAccessor> _recordAccessor;

    value::FieldAccessorMap _fieldAccessors;
    value::SlotAccessorMap _varAccessors;

    size_t _currentRange{std::numeric_limits<std::size_t>::max()};
    BsonRange _range;

    ScanStats _specificStats;
};

class BSONScanStdinStage final : public PlanStage {
public:
    BSONScanStdinStage(boost::optional<value::SlotId> recordSlot,
                       std::vector<std::string> fields,
                       value::SlotVector vars,
                       PlanNodeId planNodeId);

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats(bool /*includeDebugInfo*/) const final;
    const SpecificStats* getSpecificStats() const final;

    std::vector<DebugPrinter::Block> debugPrint() const final;

private:
    const boost::optional<value::SlotId> _recordSlot;
    const std::vector<std::string> _fields;
    const value::SlotVector _vars;

    std::unique_ptr<value::OwnedValueAccessor> _recordAccessor;

    value::FieldAccessorMap _fieldAccessors;
    value::SlotAccessorMap _varAccessors;

    ScanStats _specificStats;
};
}  // namespace sbe
}  // namespace mongo
