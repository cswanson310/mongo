/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include <stack>

#include "mongo/base/error_codes.h"
#include "mongo/db/pipeline/abt/agg_expression_visitor.h"
#include "mongo/db/pipeline/abt/expression_context.h"
#include "mongo/db/pipeline/abt/utils.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer {

class ABTAggExpressionVisitor final : public ExpressionVisitor {
public:
    ABTAggExpressionVisitor(ABTExpressionContext& ctx) : _prefixId(), _ctx(ctx){};

    void visit(ExpressionConstant* expr) override final {
        auto [tag, val] = convertFrom(expr->getValue());
        _ctx.push<Constant>(tag, val);
    }

    void visit(ExpressionAbs* expr) override final {
        pushSingleArgFunctionFromTop("abs");
    }

    void visit(ExpressionAdd* expr) override final {
        pushArithmeticBinaryExpr(expr, Operations::Add);
    }

    void visit(ExpressionAllElementsTrue* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionAnd* expr) override final {
        visitMultiBranchLogicExpression(expr, Operations::And);
    }

    void visit(ExpressionAnyElementTrue* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionArray* expr) override final {
        const size_t childCount = expr->getChildren().size();
        _ctx.ensureArity(childCount);

        // Need to process in reverse order because of the stack.
        ABTVector children;
        for (size_t i = 0; i < childCount; i++) {
            children.emplace_back(_ctx.pop());
        }

        sbe::value::Array array;
        for (size_t i = 0; i < childCount; i++) {
            ABT& child = children.at(childCount - i - 1);
            uassert(
                6624000, "Only constants are supported as array elements.", child.is<Constant>());

            auto [tag, val] = child.cast<Constant>()->get();
            // Copy the value before inserting into the array.
            auto [tagCopy, valCopy] = sbe::value::copyValue(tag, val);
            array.push_back(tagCopy, valCopy);
        }

        auto [tag, val] = sbe::value::makeCopyArray(array);
        _ctx.push<Constant>(tag, val);
    }

    void visit(ExpressionArrayElemAt* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFirst* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionLast* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionObjectToArray* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionArrayToObject* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionBsonSize* expr) override final {
        pushSingleArgFunctionFromTop("bsonSize");
    }

    void visit(ExpressionCeil* expr) override final {
        pushSingleArgFunctionFromTop("ceil");
    }

    void visit(ExpressionCoerceToBool* expr) override final {
        // Since $coerceToBool is internal-only and there are not yet any input expressions that
        // generate an ExpressionCoerceToBool expression, we will leave it as unreachable for now.
        MONGO_UNREACHABLE;
    }

    void visit(ExpressionCompare* expr) override final {
        _ctx.ensureArity(2);
        ABT right = _ctx.pop();
        ABT left = _ctx.pop();

        switch (expr->getOp()) {
            case ExpressionCompare::CmpOp::EQ:
                _ctx.push<BinaryOp>(Operations::Eq, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::NE:
                _ctx.push<BinaryOp>(Operations::Neq, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::GT:
                _ctx.push<BinaryOp>(Operations::Gt, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::GTE:
                _ctx.push<BinaryOp>(Operations::Gte, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::LT:
                _ctx.push<BinaryOp>(Operations::Lt, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::LTE:
                _ctx.push<BinaryOp>(Operations::Lte, std::move(left), std::move(right));
                break;

            case ExpressionCompare::CmpOp::CMP:
                _ctx.push<FunctionCall>("cmp3w", ABTVector{std::move(left), std::move(right)});
                break;

            default:
                MONGO_UNREACHABLE;
        }
    }

    void visit(ExpressionConcat* expr) override final {
        pushMultiArgFunctionFromTop("concat", expr->getChildren().size());
    }

    void visit(ExpressionConcatArrays* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionCond* expr) override final {
        _ctx.ensureArity(3);
        ABT cond = generateCoerceToBoolPopInput();
        ABT thenCase = _ctx.pop();
        ABT elseCase = _ctx.pop();
        _ctx.push<If>(std::move(cond), std::move(thenCase), std::move(elseCase));
    }

    void visit(ExpressionDateFromString* expr) override final {
        unsupportedExpression("$dateFromString");
    }

    void visit(ExpressionDateFromParts* expr) override final {
        unsupportedExpression("$dateFromParts");
    }

    void visit(ExpressionDateDiff* expr) override final {
        unsupportedExpression("$dateDiff");
    }

    void visit(ExpressionDateToParts* expr) override final {
        unsupportedExpression("$dateToParts");
    }

    void visit(ExpressionDateToString* expr) override final {
        unsupportedExpression("$dateToString");
    }

    void visit(ExpressionDateTrunc* expr) override final {
        unsupportedExpression("$dateTrunc");
    }

    void visit(ExpressionDivide* expr) override final {
        pushArithmeticBinaryExpr(expr, Operations::Div);
    }

    void visit(ExpressionExp* expr) override final {
        pushSingleArgFunctionFromTop("exp");
    }

    void visit(ExpressionFieldPath* expr) override final {
        const auto& varId = expr->getVariableId();
        if (Variables::isUserDefinedVariable(varId)) {
            _ctx.push<Variable>(generateVariableName(varId));
            return;
        }

        const FieldPath& fieldPath = expr->getFieldPath();
        const size_t pathLength = fieldPath.getPathLength();
        if (pathLength <= 1) {
            return;
        }

        // Here we skip over "CURRENT" first path element. This is represented by rootProjection
        // variable.
        ABT path = translateFieldPath(
            fieldPath,
            make<PathIdentity>(),
            [](const std::string& fieldName, const bool isLastElement, ABT input) {
                return make<PathGet>(fieldName,
                                     isLastElement ? std::move(input)
                                                   : make<PathTraverse>(std::move(input)));
            },
            1ul);

        _ctx.push<EvalPath>(std::move(path), make<Variable>(_ctx.getRootProjection()));
    }

    void visit(ExpressionFilter* expr) override final {
        const auto& varId = expr->getVariableId();
        uassert(6624000,
                "Filter variable must be user-defined.",
                Variables::isUserDefinedVariable(varId));
        const std::string& varName = generateVariableName(varId);

        _ctx.ensureArity(2);
        ABT filter = _ctx.pop();
        ABT input = _ctx.pop();

        _ctx.push<EvalPath>(make<PathTraverse>(make<PathLambda>(make<LambdaAbstraction>(
                                varName,
                                make<If>(generateCoerceToBoolInternal(std::move(filter)),
                                         make<Variable>(varName),
                                         Constant::nothing())))),
                            std::move(input));
    }

    void visit(ExpressionFloor* expr) override final {
        pushSingleArgFunctionFromTop("floor");
    }

    void visit(ExpressionIfNull* expr) override final {
        pushMultiArgFunctionFromTop("ifNull", 2);
    }

    void visit(ExpressionIn* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionIndexOfArray* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionIndexOfBytes* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionIndexOfCP* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionIsNumber* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionLet* expr) override final {
        unsupportedExpression("$let");
    }

    void visit(ExpressionLn* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionLog* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionLog10* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionMap* expr) override final {
        unsupportedExpression("$map");
    }

    void visit(ExpressionMeta* expr) override final {
        unsupportedExpression("$meta");
    }

    void visit(ExpressionMod* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionMultiply* expr) override final {
        pushArithmeticBinaryExpr(expr, Operations::Mult);
    }

    void visit(ExpressionNot* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionObject* expr) override final {
        const auto& expressions = expr->getChildExpressions();
        const size_t childCount = expressions.size();
        _ctx.ensureArity(childCount);

        // Need to process in reverse order because of the stack.
        ABTVector children;
        for (size_t i = 0; i < childCount; i++) {
            children.emplace_back(_ctx.pop());
        }

        sbe::value::Object object;
        for (size_t i = 0; i < childCount; i++) {
            ABT& child = children.at(childCount - i - 1);
            uassert(
                6624000, "Only constants are supported as object fields.", child.is<Constant>());

            auto [tag, val] = child.cast<Constant>()->get();
            // Copy the value before inserting into the object
            auto [tagCopy, valCopy] = sbe::value::copyValue(tag, val);
            object.push_back(expressions.at(i).first, tagCopy, valCopy);
        }

        auto [tag, val] = sbe::value::makeCopyObject(object);
        _ctx.push<Constant>(tag, val);
    }

    void visit(ExpressionOr* expr) override final {
        visitMultiBranchLogicExpression(expr, Operations::Or);
    }

    void visit(ExpressionPow* expr) override final {
        unsupportedExpression("$pow");
    }

    void visit(ExpressionRange* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionReduce* expr) override final {
        unsupportedExpression("$reduce");
    }

    void visit(ExpressionReplaceOne* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionReplaceAll* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSetDifference* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSetEquals* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSetIntersection* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSetIsSubset* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSetUnion* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSize* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionReverseArray* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSlice* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionIsArray* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionRound* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSplit* expr) override final {
        pushMultiArgFunctionFromTop("split", expr->getChildren().size());
    }

    void visit(ExpressionSqrt* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionStrcasecmp* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSubstrBytes* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSubstrCP* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionStrLenBytes* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionBinarySize* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionStrLenCP* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionSubtract* expr) override final {
        pushArithmeticBinaryExpr(expr, Operations::Sub);
    }

    void visit(ExpressionSwitch* expr) override final {
        const size_t arity = expr->getChildren().size();
        _ctx.ensureArity(arity);
        const size_t numCases = (arity - 1) / 2;

        ABTVector children;
        for (size_t i = 0; i < numCases; i++) {
            children.emplace_back(generateCoerceToBoolPopInput());
            children.emplace_back(_ctx.pop());
        }

        if (expr->getChildren().back() != nullptr) {
            children.push_back(_ctx.pop());
        }

        _ctx.push<FunctionCall>("switch", std::move(children));
    }

    void visit(ExpressionTestApiVersion* expr) override final {
        unsupportedExpression("$_testApiVersion");
    }

    void visit(ExpressionToLower* expr) override final {
        pushSingleArgFunctionFromTop("toLower");
    }

    void visit(ExpressionToUpper* expr) override final {
        pushSingleArgFunctionFromTop("toUpper");
    }

    void visit(ExpressionTrim* expr) override final {
        unsupportedExpression("$trim");
    }

    void visit(ExpressionTrunc* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionType* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionZip* expr) override final {
        unsupportedExpression("$zip");
    }

    void visit(ExpressionConvert* expr) override final {
        unsupportedExpression("$convert");
    }

    void visit(ExpressionRegexFind* expr) override final {
        unsupportedExpression("$regexFind");
    }

    void visit(ExpressionRegexFindAll* expr) override final {
        unsupportedExpression("$regexFindAll");
    }

    void visit(ExpressionRegexMatch* expr) override final {
        unsupportedExpression("$regexMatch");
    }

    void visit(ExpressionCosine* expr) override final {
        pushSingleArgFunctionFromTop("cosine");
    }

    void visit(ExpressionSine* expr) override final {
        pushSingleArgFunctionFromTop("sine");
    }

    void visit(ExpressionTangent* expr) override final {
        pushSingleArgFunctionFromTop("tangent");
    }

    void visit(ExpressionArcCosine* expr) override final {
        pushSingleArgFunctionFromTop("arcCosine");
    }

    void visit(ExpressionArcSine* expr) override final {
        pushSingleArgFunctionFromTop("arcSine");
    }

    void visit(ExpressionArcTangent* expr) override final {
        pushSingleArgFunctionFromTop("arcTangent");
    }

    void visit(ExpressionArcTangent2* expr) override final {
        pushSingleArgFunctionFromTop("arcTangent2");
    }

    void visit(ExpressionHyperbolicArcTangent* expr) override final {
        pushSingleArgFunctionFromTop("arcTangentH");
    }

    void visit(ExpressionHyperbolicArcCosine* expr) override final {
        pushSingleArgFunctionFromTop("arcCosineH");
    }

    void visit(ExpressionHyperbolicArcSine* expr) override final {
        pushSingleArgFunctionFromTop("arcSineH");
    }

    void visit(ExpressionHyperbolicTangent* expr) override final {
        pushSingleArgFunctionFromTop("tangentH");
    }

    void visit(ExpressionHyperbolicCosine* expr) override final {
        pushSingleArgFunctionFromTop("cosineH");
    }

    void visit(ExpressionHyperbolicSine* expr) override final {
        pushSingleArgFunctionFromTop("sineH");
    }

    void visit(ExpressionDegreesToRadians* expr) override final {
        pushSingleArgFunctionFromTop("degreesToRadians");
    }

    void visit(ExpressionRadiansToDegrees* expr) override final {
        pushSingleArgFunctionFromTop("radiansToDegrees");
    }

    void visit(ExpressionDayOfMonth* expr) override final {
        unsupportedExpression("$dayOfMonth");
    }

    void visit(ExpressionDayOfWeek* expr) override final {
        unsupportedExpression("$dayOfWeek");
    }

    void visit(ExpressionDayOfYear* expr) override final {
        unsupportedExpression("$dayOfYear");
    }

    void visit(ExpressionHour* expr) override final {
        unsupportedExpression("$hour");
    }

    void visit(ExpressionMillisecond* expr) override final {
        unsupportedExpression("$millisecond");
    }

    void visit(ExpressionMinute* expr) override final {
        unsupportedExpression("$minute");
    }

    void visit(ExpressionMonth* expr) override final {
        unsupportedExpression("$month");
    }

    void visit(ExpressionSecond* expr) override final {
        unsupportedExpression("$second");
    }

    void visit(ExpressionWeek* expr) override final {
        unsupportedExpression("$week");
    }

    void visit(ExpressionIsoWeekYear* expr) override final {
        unsupportedExpression("$isoWeekYear");
    }

    void visit(ExpressionIsoDayOfWeek* expr) override final {
        unsupportedExpression("$isoDayOfWeek");
    }

    void visit(ExpressionIsoWeek* expr) override final {
        unsupportedExpression("$isoWeek");
    }

    void visit(ExpressionYear* expr) override final {
        unsupportedExpression("$year");
    }

    void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionTests::Testable* expr) override final {
        unsupportedExpression("$test");
    }

    void visit(ExpressionInternalJsEmit* expr) override final {
        unsupportedExpression("$internalJsEmit");
    }

    void visit(ExpressionInternalFindSlice* expr) override final {
        unsupportedExpression("$internalFindSlice");
    }

    void visit(ExpressionInternalFindPositional* expr) override final {
        unsupportedExpression("$internalFindPositional");
    }

    void visit(ExpressionInternalFindElemMatch* expr) override final {
        unsupportedExpression("$internalFindElemMatch");
    }

    void visit(ExpressionFunction* expr) override final {
        unsupportedExpression("$function");
    }

    void visit(ExpressionRandom* expr) override final {
        unsupportedExpression(expr->getOpName());
    }

    void visit(ExpressionToHashedIndexKey* expr) override final {
        unsupportedExpression("$toHashedIndexKey");
    }

    void visit(ExpressionDateAdd* expr) override final {
        unsupportedExpression("dateAdd");
    }

    void visit(ExpressionDateSubtract* expr) override final {
        unsupportedExpression("dateSubtract");
    }

private:
    /**
     * Shared logic for $and, $or. Converts each child into an EExpression that evaluates to Boolean
     * true or false, based on MQL rules for $and and $or branches, and then chains the branches
     * together using binary and/or EExpressions so that the result has MQL's short-circuit
     * semantics.
     */
    void visitMultiBranchLogicExpression(Expression* expr, Operations logicOp) {
        invariant(logicOp == Operations::And || logicOp == Operations::Or);
        const size_t arity = expr->getChildren().size();
        _ctx.ensureArity(arity);

        if (arity == 0) {
            // Empty $and and $or always evaluate to their logical operator's identity value: true
            // and false, respectively.
            const bool logicIdentityVal = (logicOp == Operations::And);
            _ctx.push<Constant>(sbe::value::TypeTags::Boolean,
                                sbe::value::bitcastFrom<bool>(logicIdentityVal));
            return;
        }

        ABT current = generateCoerceToBoolPopInput();
        for (size_t i = 0; i < arity - 1; i++) {
            current = make<BinaryOp>(logicOp, std::move(current), generateCoerceToBoolPopInput());
        }
        _ctx.push(std::move(current));
    }

    void pushMultiArgFunctionFromTop(const std::string& functionName, const size_t argCount) {
        _ctx.ensureArity(argCount);

        ABTVector children;
        for (size_t i = 0; i < argCount; i++) {
            children.emplace_back(_ctx.pop());
        }
        _ctx.push<FunctionCall>(functionName, children);
    }

    void pushSingleArgFunctionFromTop(const std::string& functionName) {
        pushMultiArgFunctionFromTop(functionName, 1);
    }

    void pushArithmeticBinaryExpr(Expression* expr, const Operations op) {
        const size_t arity = expr->getChildren().size();
        _ctx.ensureArity(arity);
        if (arity < 2) {
            // Nothing to do for arity 0 and 1.
            return;
        }

        ABT current = _ctx.pop();
        for (size_t i = 0; i < arity - 1; i++) {
            current = make<BinaryOp>(op, std::move(current), _ctx.pop());
        }
        _ctx.push(std::move(current));
    }

    ABT generateCoerceToBoolInternal(ABT input) {
        return generateCoerceToBool(std::move(input), getNextId("coerceToBool"));
    }

    ABT generateCoerceToBoolPopInput() {
        return generateCoerceToBoolInternal(_ctx.pop());
    }

    std::string generateVariableName(const Variables::Id varId) {
        std::ostringstream os;
        os << _ctx.getUniqueIdPrefix() << "_var_" << varId;
        return os.str();
    }

    std::string getNextId(const std::string& key) {
        return _ctx.getUniqueIdPrefix() + "_" + _prefixId.getNextId(key);
    }

    void unsupportedExpression(const char* op) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Expression is not supported: " << op);
    }

    PrefixId _prefixId;

    // We don't own this.
    ABTExpressionContext& _ctx;
};

class AggExpressionWalker final {
public:
    AggExpressionWalker(ABTAggExpressionVisitor* visitor) : _visitor{visitor} {}

    void preVisit(Expression* expr) {
        // noop
    }

    void inVisit(long long /*count*/, Expression* expr) {
        // noop
    }

    void postVisit(Expression* expr) {
        expr->acceptVisitor(_visitor);
    }

private:
    ABTAggExpressionVisitor* _visitor;
};

ABT generateAggExpression(Expression* expr,
                          const std::string& rootProjection,
                          const std::string& uniqueIdPrefix) {
    ABTExpressionContext ctx(
        true /*assertExprSort*/, false /*assertPathSort*/, rootProjection, uniqueIdPrefix);
    ABTAggExpressionVisitor visitor(ctx);

    AggExpressionWalker walker(&visitor);
    expression_walker::walk(&walker, expr);
    return ctx.pop();
}

}  // namespace mongo::optimizer
