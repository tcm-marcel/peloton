//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.h
//
// Identification: src/include/codegen/interpreter/query_interpreter.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query.h"

namespace peloton {
namespace codegen {
namespace interpreter {

class QueryInterpreter {
 public:
  static bool IsSupported(const Query::QueryFunctions &query_functions);
  static bool IsExpressionSupported(
      const Query::QueryFunctions &query_functions);

 private:
  friend class Query;
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
