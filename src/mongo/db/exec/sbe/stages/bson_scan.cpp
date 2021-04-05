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

#ifndef _WIN32
#include <cstdio>
#endif

#include "mongo/db/exec/sbe/stages/bson_scan.h"

#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/util/str.h"

#ifdef _WIN32
typedef SSIZE_T ssize_t;
#endif

namespace mongo {
namespace sbe {
BSONScanStage::BSONScanStage(const char* bsonBegin,
                             const char* bsonEnd,
                             boost::optional<value::SlotId> recordSlot,
                             std::vector<std::string> fields,
                             value::SlotVector vars,
                             PlanNodeId planNodeId)
    : PlanStage("bsonscan"_sd, planNodeId),
      _bsonBegin(bsonBegin),
      _bsonEnd(bsonEnd),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)),
      _bsonCurrent(bsonBegin) {}

BSONScanStage::BSONScanStage(std::string fileName,
                             boost::optional<value::SlotId> recordSlot,
                             std::vector<std::string> fields,
                             value::SlotVector vars,
                             PlanNodeId planNodeId)
    : PlanStage("bsonscan"_sd, planNodeId),
      _fileName(fileName),
      _bsonBegin(nullptr),
      _bsonEnd(nullptr),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)),
      _bsonCurrent(nullptr) {}


std::unique_ptr<PlanStage> BSONScanStage::clone() const {
    return _fileName.empty()
        ? std::make_unique<BSONScanStage>(
              _bsonBegin, _bsonEnd, _recordSlot, _fields, _vars, _commonStats.nodeId)
        : std::make_unique<BSONScanStage>(
              _fileName, _recordSlot, _fields, _vars, _commonStats.nodeId);
}

void BSONScanStage::prepare(CompileCtx& ctx) {
    if (_recordSlot) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        auto [it, inserted] =
            _fieldAccessors.emplace(_fields[idx], std::make_unique<value::ViewOfValueAccessor>());
        uassert(4822841, str::stream() << "duplicate field: " << _fields[idx], inserted);
        auto [itRename, insertedRename] = _varAccessors.emplace(_vars[idx], it->second.get());
        uassert(4822842, str::stream() << "duplicate field: " << _vars[idx], insertedRename);
    }
}

value::SlotAccessor* BSONScanStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_recordSlot && *_recordSlot == slot) {
        return _recordAccessor.get();
    }

    if (auto it = _varAccessors.find(slot); it != _varAccessors.end()) {
        return it->second;
    }

    return ctx.getAccessor(slot);
}

void BSONScanStage::open(bool reOpen) {
    auto optTimer(getOptTimer(_opCtx));

    if (!_fileName.empty() && !_mappedFile.is_open()) {
        try {
            _mappedFile.open(_fileName);
            _bsonBegin = _mappedFile.data();
            _bsonEnd = _bsonBegin + _mappedFile.size();
        } catch (const std::ios::failure& e) {
            std::cerr << e.what() << "\n";
            _bsonBegin = _bsonEnd = nullptr;
        }
    }

    _commonStats.opens++;
    _bsonCurrent = _bsonBegin;
}

PlanState BSONScanStage::getNext() {
    auto optTimer(getOptTimer(_opCtx));

    if (_bsonCurrent < _bsonEnd) {
        if (_recordAccessor) {
            _recordAccessor->reset(value::TypeTags::bsonObject,
                                   value::bitcastFrom<const char*>(_bsonCurrent));
        }

        if (auto fieldsToMatch = _fieldAccessors.size(); fieldsToMatch != 0) {
            auto be = _bsonCurrent;
            auto end = be + ConstDataView(be).read<LittleEndian<uint32_t>>();
            // Skip document length.
            be += 4;
            for (auto& [name, accessor] : _fieldAccessors) {
                accessor->reset();
            }
            while (*be != 0) {
                auto sv = bson::fieldNameView(be);
                if (auto it = _fieldAccessors.find(sv); it != _fieldAccessors.end()) {
                    // Found the field so convert it to Value.
                    auto [tag, val] = bson::convertFrom(true, be, end, sv.size());

                    it->second->reset(tag, val);

                    if ((--fieldsToMatch) == 0) {
                        // No need to scan any further so bail out early.
                        break;
                    }
                }

                be = bson::advance(be, sv.size());
            }
        }

        // Advance to the next document.
        _bsonCurrent += ConstDataView(_bsonCurrent).read<LittleEndian<uint32_t>>();

        _specificStats.numReads++;
        return trackPlanState(PlanState::ADVANCED);
    }

    return trackPlanState(PlanState::IS_EOF);
}

void BSONScanStage::close() {
    auto optTimer(getOptTimer(_opCtx));

    trackClose();

    if (!_fileName.empty() && _mappedFile.is_open()) {
        _mappedFile.close();
    }
    _commonStats.closes++;
}

std::unique_ptr<PlanStageStats> BSONScanStage::getStats(bool includeDebugInfo) const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->specific = std::make_unique<ScanStats>(_specificStats);

    if (includeDebugInfo) {
        BSONObjBuilder bob;
        if (_recordSlot) {
            bob.appendNumber("recordSlot", static_cast<long long>(*_recordSlot));
        }
        bob.append("field", _fields);
        bob.append("outputSlots", _vars);
        ret->debugInfo = bob.obj();
    }

    return ret;
}

const SpecificStats* BSONScanStage::getSpecificStats() const {
    return &_specificStats;
}

std::vector<DebugPrinter::Block> BSONScanStage::debugPrint() const {
    auto ret = PlanStage::debugPrint();

    if (_recordSlot) {
        DebugPrinter::addIdentifier(ret, _recordSlot.get());
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _vars[idx]);
        ret.emplace_back("=");
        DebugPrinter::addIdentifier(ret, _fields[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    return ret;
}

ParallelBsonScanStage::ParallelBsonScanStage(const char* bsonBegin,
                                             const char* bsonEnd,
                                             boost::optional<value::SlotId> recordSlot,
                                             std::vector<std::string> fields,
                                             value::SlotVector vars,
                                             PlanNodeId planNodeId)
    : PlanStage("pbsonscan"_sd, planNodeId),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)) {
    _state = std::make_unique<ParallelState>(bsonBegin, bsonEnd);
}

ParallelBsonScanStage::ParallelBsonScanStage(std::string fileName,
                                             boost::optional<value::SlotId> recordSlot,
                                             std::vector<std::string> fields,
                                             value::SlotVector vars,
                                             PlanNodeId planNodeId)
    : PlanStage("pbsonscan"_sd, planNodeId),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)) {
    _state = std::make_unique<ParallelState>(fileName);
}

ParallelBsonScanStage::ParallelBsonScanStage(const std::shared_ptr<ParallelState>& state,
                                             boost::optional<value::SlotId> recordSlot,
                                             std::vector<std::string> fields,
                                             value::SlotVector vars,
                                             PlanNodeId planNodeId)
    : PlanStage("pbsonscan"_sd, planNodeId),
      _state(state),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)) {}

std::unique_ptr<PlanStage> ParallelBsonScanStage::clone() const {
    return std::make_unique<ParallelBsonScanStage>(
        _state, _recordSlot, _fields, _vars, _commonStats.nodeId);
}

void ParallelBsonScanStage::prepare(CompileCtx& ctx) {
    if (_recordSlot) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        auto [it, inserted] =
            _fieldAccessors.emplace(_fields[idx], std::make_unique<value::ViewOfValueAccessor>());
        uassert(4822841, str::stream() << "duplicate field: " << _fields[idx], inserted);
        auto [itRename, insertedRename] = _varAccessors.emplace(_vars[idx], it->second.get());
        uassert(4822842, str::stream() << "duplicate field: " << _vars[idx], insertedRename);
    }
}

value::SlotAccessor* ParallelBsonScanStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_recordSlot && *_recordSlot == slot) {
        return _recordAccessor.get();
    }

    if (auto it = _varAccessors.find(slot); it != _varAccessors.end()) {
        return it->second;
    }

    return ctx.getAccessor(slot);
}

void ParallelBsonScanStage::open(bool reOpen) {
    invariant(!reOpen, "parallel scan is not restartable");
    stdx::unique_lock lock(_state->mutex);
    if (_state->ranges.empty()) {
        // Random first threads reads in the whole bson and establishes ranges
        if (!_state->fileName.empty() && !_state->mappedFile.is_open()) {
            try {
                _state->mappedFile.open(_state->fileName);
                _state->bsonBegin = _state->mappedFile.data();
                _state->bsonEnd = _state->bsonBegin + _state->mappedFile.size();
            } catch (const std::ios::failure& e) {
                std::cerr << e.what() << "\n";
                _state->bsonBegin = _state->bsonEnd = nullptr;
            }
        }
        // Construct ranges
        if (_state->bsonBegin) {
            auto current = _state->bsonBegin;
            while (current < _state->bsonEnd) {
                BsonRange r;
                r.count = 0;
                r.begin = current;
                while (r.count < 10240 && current < _state->bsonEnd) {
                    auto docSize = ConstDataView(current).read<LittleEndian<std::uint32_t>>();
                    current += docSize;
                    ++r.count;
                }
                r.end = current;

                _state->ranges.emplace_back(std::move(r));
            }
        }
        _state->currentRange.store(0);
    }
    nextRange();

    _commonStats.opens++;
}

PlanState ParallelBsonScanStage::getNext() {
    while (_currentRange < _state->ranges.size()) {
        auto current = _range.begin;
        if (current < _range.end) {
            if (_recordAccessor) {
                _recordAccessor->reset(value::TypeTags::bsonObject,
                                       value::bitcastFrom<const char*>(current));
            }

            if (auto fieldsToMatch = _fieldAccessors.size(); fieldsToMatch != 0) {
                auto be = current + 4;
                auto end = current + ConstDataView(current).read<LittleEndian<std::uint32_t>>();
                for (auto& [name, accessor] : _fieldAccessors) {
                    accessor->reset();
                }
                while (*be != 0) {
                    auto sv = bson::fieldNameView(be);
                    if (auto it = _fieldAccessors.find(sv); it != _fieldAccessors.end()) {
                        // Found the field so convert it to Value.
                        auto [tag, val] = bson::convertFrom(true, be, end, sv.size());

                        it->second->reset(tag, val);

                        if ((--fieldsToMatch) == 0) {
                            // No need to scan any further so bail out early.
                            break;
                        }
                    }

                    be = bson::advance(be, sv.size());
                }
            }

            // Advance to the next document.
            current += ConstDataView(current).read<LittleEndian<std::uint32_t>>();

            _specificStats.numReads++;
            return trackPlanState(PlanState::ADVANCED);
        } else {
            nextRange();
        }
    }
    _commonStats.isEOF = true;
    return trackPlanState(PlanState::IS_EOF);
}

void ParallelBsonScanStage::close() {
    _commonStats.closes++;
}

std::unique_ptr<PlanStageStats> ParallelBsonScanStage::getStats(bool /*includeDebugInfo*/) const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->specific = std::make_unique<ScanStats>(_specificStats);
    return ret;
}

const SpecificStats* ParallelBsonScanStage::getSpecificStats() const {
    return &_specificStats;
}

std::vector<DebugPrinter::Block> ParallelBsonScanStage::debugPrint() const {
    auto ret = PlanStage::debugPrint();

    if (_recordSlot) {
        DebugPrinter::addIdentifier(ret, _recordSlot.get());
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _vars[idx]);
        ret.emplace_back("=");
        DebugPrinter::addIdentifier(ret, _fields[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    return ret;
}


BSONScanStdinStage::BSONScanStdinStage(boost::optional<value::SlotId> recordSlot,
                                       std::vector<std::string> fields,
                                       value::SlotVector vars,
                                       PlanNodeId planNodeId)
    : PlanStage("bsonscanstdio"_sd, planNodeId),
      _recordSlot(recordSlot),
      _fields(std::move(fields)),
      _vars(std::move(vars)) {}

std::unique_ptr<PlanStage> BSONScanStdinStage::clone() const {
    return std::make_unique<BSONScanStdinStage>(_recordSlot, _fields, _vars, _commonStats.nodeId);
}

void BSONScanStdinStage::prepare(CompileCtx& ctx) {
    if (_recordSlot) {
        _recordAccessor = std::make_unique<value::OwnedValueAccessor>();
    }

    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        auto [it, inserted] =
            _fieldAccessors.emplace(_fields[idx], std::make_unique<value::ViewOfValueAccessor>());
        uassert(4822841, str::stream() << "duplicate field: " << _fields[idx], inserted);
        auto [itRename, insertedRename] = _varAccessors.emplace(_vars[idx], it->second.get());
        uassert(4822842, str::stream() << "duplicate field: " << _vars[idx], insertedRename);
    }
}

value::SlotAccessor* BSONScanStdinStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_recordSlot && *_recordSlot == slot) {
        return _recordAccessor.get();
    }

    if (auto it = _varAccessors.find(slot); it != _varAccessors.end()) {
        return it->second;
    }

    return ctx.getAccessor(slot);
}

void BSONScanStdinStage::open(bool reOpen) {
    uassert(0, "stdin bson scan cannot be reopened", !reOpen);

#ifndef _WIN32
    // Reopen stdin in binary mode.
    FILE* fh = std::freopen(nullptr, "rb", stdin);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to reopen stdin for binary data: " << strerror(errno),
            fh != nullptr);

    // Set buffer size to default pipe buffer size on Linux.
    int ok = std::setvbuf(stdin, nullptr, _IOFBF, 65536);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to resize stdin buffer: " << strerror(errno),
            ok == 0);
#endif

    _commonStats.opens++;
}

static ssize_t readStdin(void* buf, size_t count) {
#ifdef _WIN32
    unsigned long bytesRead;
    HANDLE hFile = GetStdHandle(STD_INPUT_HANDLE);
    if (!ReadFile(hFile, buf, static_cast<unsigned long>(count), &bytesRead, nullptr)) {
        return GetLastError() == ERROR_BROKEN_PIPE ? 0 : -1;
    }
    return static_cast<ssize_t>(bytesRead);
#else
    auto n = std::fread(buf, 1, count, stdin);

    if (n > 0)
        return n;

    if (std::feof(stdin))
        return 0;

    return -1;
#endif
}

PlanState BSONScanStdinStage::getNext() {
    char sizeBuf[4];
    auto n = readStdin(&sizeBuf, sizeof(sizeBuf));
    if (n == 0) {
        _commonStats.isEOF = true;
        return trackPlanState(PlanState::IS_EOF);
    } else if (n != sizeof(sizeBuf)) {
        std::cerr << "error reading from stdin" << std::endl;
        _commonStats.isEOF = true;
        return trackPlanState(PlanState::IS_EOF);
    }

    uint32_t size = ConstDataView(sizeBuf).read<LittleEndian<std::uint32_t>>();
    uassert(0,
            str::stream() << "Input BSON document has size " << size
                          << ", which exceeds maximum allowed object size",
            size < BSONObjMaxInternalSize);

    // Must match value::releaseValue
    auto buf = UniqueBuffer::allocate(size).release();
    auto tag = value::TypeTags::bsonObject;
    auto val = value::bitcastFrom<const char*>(buf);
    value::ValueGuard guard{tag, val};

    memcpy(buf, sizeBuf, sizeof(sizeBuf));
    size_t totalRead = sizeof(sizeBuf);
    while ((n = readStdin(buf + totalRead, size - totalRead)) > 0) {
        totalRead += n;
        if (totalRead == size) {
            break;
        }
    }
    if (n < 0) {
        std::cerr << "error reading from stdin" << std::endl;
        _commonStats.isEOF = true;
        return trackPlanState(PlanState::IS_EOF);
    }
    if (n == 0) {
        invariant(totalRead < size);
        std::cerr << "unexpected end of document" << std::endl;
        _commonStats.isEOF = true;
        return trackPlanState(PlanState::IS_EOF);
    }
    invariant(totalRead == size);

    if (_recordAccessor) {
        guard.reset();
        _recordAccessor->reset(true, tag, val);
    }

    if (auto fieldsToMatch = _fieldAccessors.size(); fieldsToMatch != 0) {
        const char* be = buf + 4;
        const char* end = buf + ConstDataView(buf).read<LittleEndian<std::uint32_t>>();
        for (auto& [name, accessor] : _fieldAccessors) {
            accessor->reset();
        }
        while (*be != 0) {
            auto sv = bson::fieldNameView(be);
            if (auto it = _fieldAccessors.find(sv); it != _fieldAccessors.end()) {
                // Found the field so convert it to Value.
                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());

                it->second->reset(tag, val);

                if ((--fieldsToMatch) == 0) {
                    // No need to scan any further so bail out early.
                    break;
                }
            }

            be = bson::advance(be, sv.size());
        }
    }

    _specificStats.numReads++;
    return trackPlanState(PlanState::ADVANCED);
}

void BSONScanStdinStage::close() {
    _commonStats.closes++;
}

std::unique_ptr<PlanStageStats> BSONScanStdinStage::getStats(bool /*includeDebugInfo*/) const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->specific = std::make_unique<ScanStats>(_specificStats);
    return ret;
}

const SpecificStats* BSONScanStdinStage::getSpecificStats() const {
    return &_specificStats;
}

std::vector<DebugPrinter::Block> BSONScanStdinStage::debugPrint() const {
    auto ret = PlanStage::debugPrint();

    if (_recordSlot) {
        DebugPrinter::addIdentifier(ret, _recordSlot.get());
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _vars[idx]);
        ret.emplace_back("=");
        DebugPrinter::addIdentifier(ret, _fields[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    return ret;
}

}  // namespace sbe
}  // namespace mongo
