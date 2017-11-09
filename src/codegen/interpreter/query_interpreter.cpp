//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.cpp
//
// Identification: src/codegen/interpreter/query_interpreter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/query_interpreter.h"

namespace peloton {
namespace codegen {
namespace interpreter {

bool QueryInterpreter::IsSupported(
    const Query::QueryFunctions &query_functions) {
  (void)query_functions;
  return false;
}

bool QueryInterpreter::IsExpressionSupported(
    const Query::QueryFunctions &query_functions) {
  (void)query_functions;
  return false;
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
