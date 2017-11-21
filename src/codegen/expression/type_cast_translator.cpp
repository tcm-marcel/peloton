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

#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/type/integer_type.h"
#include "codegen/expression/type_cast_translator.h"
#include "expression/typecast_expression.h"

namespace peloton {
namespace codegen {

// Constructor
TypeCastTranslator::TypeCastTranslator(
    const expression::TypecastExpression &expr, CompilationContext &context)
    : ExpressionTranslator(expr, context) {
  // TODO(marcel): add additional checks here if cast is possible? or do this in DeriveValue?
}

// Produce the value that is the result of codegening the expression
codegen::Value TypeCastTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &row) const {
  const auto &expr = GetExpressionAs<expression::TypecastExpression>();
  type::Type type_to = expr.ResultType();
  type::Type type_from = expr.GetChild(0)->ResultType();
  Value value_from = row.DeriveValue(codegen, *expr.GetChild(0));

  // If source and destination type is equal, skip cast
  if (type_from == type_to)
    return value_from;

  // TODO(marcel): am I allowed to access type_id directly?
  // Decide which cast to use depending on source and destination type
  switch (type_from.type_id) {
    // Casts from varchar
    case ::peloton::type::TypeId::VARCHAR: {
      switch (type_to.type_id) {
        case ::peloton::type::TypeId::INTEGER: {
          llvm::Value *raw_value = codegen.Call(StringFunctionsProxy::CastToInt, {value_from.GetValue(), value_from.GetLength()});
          return Value{type::Integer::Instance(), raw_value};
        }

        default:
          goto overall_default;
      }
    }

    // Casts from integer
    case ::peloton::type::TypeId::INTEGER: {
      switch (type_to.type_id) {
        case ::peloton::type::TypeId::DECIMAL:
        case ::peloton::type::TypeId::SMALLINT: {
          return value_from.CastTo(codegen, type_to);
        }

        default:
          goto overall_default;
      }
    }

    // This is a label that collects all default branches of the inner switch cases
      // Looks like 1876 but works and avoids duplicated code
    overall_default:
    default:
      throw Exception("No explicit cast available for " + TypeIdToString(type_from.type_id) + " to " + TypeIdToString(type_to.type_id));
  }
}

}  // namespace codegen
}  // namespace peloton