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

#include "mongo/db/pipeline/mongo_process_common.h"

#include "mongo/bson/mutable/document.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/service_context.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/util/net/socket_utils.h"

namespace mongo {

std::vector<BSONObj> MongoProcessCommon::getCurrentOps(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    CurrentOpConnectionsMode connMode,
    CurrentOpSessionsMode sessionMode,
    CurrentOpUserMode userMode,
    CurrentOpTruncateMode truncateMode,
    CurrentOpCursorMode cursorMode,
    CurrentOpBacktraceMode backtraceMode) const {
    OperationContext* opCtx = expCtx->opCtx;
    AuthorizationSession* ctxAuth = AuthorizationSession::get(opCtx->getClient());

    std::vector<BSONObj> ops;

    for (ServiceContext::LockedClientsCursor cursor(opCtx->getClient()->getServiceContext());
         Client* client = cursor.next();) {
        invariant(client);

        stdx::lock_guard<Client> lk(*client);

        // If auth is disabled, ignore the allUsers parameter.
        if (ctxAuth->getAuthorizationManager().isAuthEnabled() &&
            userMode == CurrentOpUserMode::kExcludeOthers &&
            !ctxAuth->isCoauthorizedWithClient(client, lk)) {
            continue;
        }

        // Ignore inactive connections unless 'idleConnections' is true.
        if (!client->getOperationContext() && connMode == CurrentOpConnectionsMode::kExcludeIdle) {
            continue;
        }

        // Delegate to the mongoD- or mongoS-specific implementation of _reportCurrentOpForClient.
        ops.emplace_back(_reportCurrentOpForClient(opCtx, client, truncateMode, backtraceMode));
    }

    // If 'cursorMode' is set to include idle cursors, retrieve them and add them to ops.
    if (cursorMode == CurrentOpCursorMode::kIncludeCursors) {

        for (auto&& cursor : getIdleCursors(expCtx, userMode)) {
            BSONObjBuilder cursorObj;
            cursorObj.append("type", "idleCursor");
            cursorObj.append("host", getHostNameCachedAndPort());
            // First, extract fields which need to go at the top level out of the GenericCursor.
            auto ns = cursor.getNs();
            cursorObj.append("ns", ns->toString());
            if (auto lsid = cursor.getLsid()) {
                cursorObj.append("lsid", lsid->toBSON());
            }
            if (auto planSummaryData = cursor.getPlanSummary()) {  // Not present on mongos.
                cursorObj.append("planSummary", *planSummaryData);
            }

            // Next, append the stripped-down version of the generic cursor. This will avoid
            // duplicating information reported at the top level.
            cursorObj.append("cursor",
                             CurOp::truncateAndSerializeGenericCursor(&cursor, boost::none));

            ops.emplace_back(cursorObj.obj());
        }
    }

    // If we need to report on idle Sessions, defer to the mongoD or mongoS implementations.
    if (sessionMode == CurrentOpSessionsMode::kIncludeIdle) {
        _reportCurrentOpsForIdleSessions(opCtx, userMode, &ops);
    }

    return ops;
}

std::vector<FieldPath> MongoProcessCommon::collectDocumentKeyFieldsActingAsRouter(
    OperationContext* opCtx, const NamespaceString& nss) const {
    if (auto chunkManager =
            uassertStatusOK(Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfo(opCtx, nss))
                .cm()) {
        return _shardKeyToDocumentKeyFields(
            chunkManager->getShardKeyPattern().getKeyPatternFields());
    }
    // We have no evidence this collection is sharded, so the document key is just _id.
    return {"_id"};
}

bool MongoProcessCommon::keyPatternNamesExactPaths(const BSONObj& keyPattern,
                                                   const std::set<FieldPath>& uniqueKeyPaths) {
    size_t nFieldsMatched = 0;
    for (auto&& elem : keyPattern) {
        if (!elem.isNumber()) {
            return false;
        }
        if (uniqueKeyPaths.find(elem.fieldNameStringData()) == uniqueKeyPaths.end()) {
            return false;
        }
        ++nFieldsMatched;
    }
    return nFieldsMatched == uniqueKeyPaths.size();
}

boost::optional<ChunkVersion> MongoProcessCommon::refreshAndGetCollectionVersion(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const NamespaceString& nss) const {
    const bool forceRefreshFromThisThread = false;
    auto routingInfo = uassertStatusOK(
        Grid::get(expCtx->opCtx)
            ->catalogCache()
            ->getCollectionRoutingInfoWithRefresh(expCtx->opCtx, nss, forceRefreshFromThisThread));
    if (auto chunkManager = routingInfo.cm()) {
        return chunkManager->getVersion();
    }
    return boost::none;
}

std::vector<FieldPath> MongoProcessCommon::_shardKeyToDocumentKeyFields(
    const std::vector<std::unique_ptr<FieldRef>>& keyPatternFields) const {
    std::vector<FieldPath> result;
    bool gotId = false;
    for (auto& field : keyPatternFields) {
        result.emplace_back(field->dottedField());
        gotId |= (result.back().fullPath() == "_id");
    }
    if (!gotId) {  // If not part of the shard key, "_id" comes last.
        result.emplace_back("_id");
    }
    return result;
}

std::set<FieldPath> MongoProcessCommon::_convertToFieldPaths(
    const std::vector<std::string>& fields) const {
    std::set<FieldPath> fieldPaths;

    for (const auto& field : fields) {
        const auto res = fieldPaths.insert(FieldPath(field));
        uassert(ErrorCodes::BadValue,
                str::stream() << "Found a duplicate field '" << field << "'",
                res.second);
    }
    return fieldPaths;
}

void MongoProcessCommon::createCollection(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                          const NamespaceString& nss,
                                          BSONObj options) {
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("create", nss.coll());
    cmdBuilder.appendElementsUnique(options);
    mongo::createCollection(expCtx->opCtx, nss, cmdBuilder.obj());
}

}  // namespace mongo
