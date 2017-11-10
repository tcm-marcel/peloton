//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_cast_translator.cpp
//
// Identification: src/codegen/expression/type_cast_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/type_cast_translator.h"
#include "expression/typecast_expression.h"

namespace peloton {
namespace codegen {

// Constructor
TypeCastTranslator::TypeCastTranslator(
    const expression::TypecastExpression &expr, CompilationContext &context)
    : ExpressionTranslator(expr, context) {
  // TODO(marcel): add additional here checks if cast is possible?
}

// Produce the value that is the result of codegening the expression
codegen::Value TypeCastTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &row) const {
  const auto &expr = GetExpressionAs<expression::TypecastExpression>();
  type::Type type_to = expr.ResultType();
  type::Type type_from = expr.GetChild(0)->ResultType();
  Value value_from = row.DeriveValue(codegen, *expr.GetChild(0));

  (void)type_from;
  Value value_to = value_from.CastTo(codegen, type_to);

  return value_to;
}

}  // namespace codegen
}  // namespace peloton