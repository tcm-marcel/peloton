//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_cast_translator.h
//
// Identification: src/include/codegen/expression/type_cast_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression/expression_translator.h"

namespace peloton {

namespace expression {
class TypecastExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator tor explicit type casts
//===----------------------------------------------------------------------===//
class TypeCastTranslator : public ExpressionTranslator {
 public:
  // Constructor
  TypeCastTranslator(const expression::TypecastExpression &expr,
                     CompilationContext &context);

  // Return the attribute from the row
  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton