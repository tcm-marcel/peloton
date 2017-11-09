//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query.h
//
// Identification: src/include/codegen/query.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "codegen/runtime_state.h"
#include "storage/storage_manager.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A query statement that can be compiled
//===----------------------------------------------------------------------===//
class Query {
 public:
  struct RuntimeStats {
    double jit_compile_ms = 0.0;
    double init_ms = 0.0;
    double plan_ms = 0.0;
    double tear_down_ms = 0.0;
  };

  // Struct holding the llvm functions of this query
  struct QueryFunctions {
    llvm::Function *init_func;
    llvm::Function *plan_func;
    llvm::Function *tear_down_func;
  };

  // We use this handy class for the parameters to the llvm functions
  // to avoid complex casting and pointer manipulation
  struct FunctionArguments {
    concurrency::Transaction *txn;
    storage::StorageManager *catalog;
    executor::ExecutorContext *executor_context;
    char *consumer_arg;
    char rest[0];
  } PACKED;

  // Setup this query statement with the given LLVM function components. The
  // provided functions perform initialization, execution and tear down of
  // this query.
  void Prepare(const QueryFunctions &funcs);

  // Execute th e query given the catalog manager and runtime/consumer state
  // that is passed along to the query execution code. The decision whether
  // to JIT compile or to interpret the query plan is made in here.
  void Execute(concurrency::Transaction &txn,
               executor::ExecutorContext *executor_context, char *consumer_arg,
               RuntimeStats *stats = nullptr);

  // Return the query plan
  const planner::AbstractPlan &GetPlan() const { return query_plan_; }

  // Get the holder of the code
  CodeContext &GetCodeContext() { return code_context_; }

  // The class tracking all the state needed by this query
  RuntimeState &GetRuntimeState() { return runtime_state_; }

 private:
  friend class QueryCompiler;

  // Constructor
  Query(const planner::AbstractPlan &query_plan);

  // Compile the ir and execute it (compilation path)
  bool CompileAndExecute(FunctionArguments *function_arguments,
                         RuntimeStats *stats);

  // Interpret the ir (interpretation path)
  bool Interpret(FunctionArguments *function_arguments, RuntimeStats *stats);

 private:
  // The query plan
  const planner::AbstractPlan &query_plan_;

  // The code context where the compiled code for the query goes
  CodeContext code_context_;

  // The size of the parameter the functions take
  RuntimeState runtime_state_;

  // The llvm ir of the init(), plan() and tearDown() functions
  QueryFunctions query_funcs_;

  typedef void (*compiled_function_t)(FunctionArguments *);

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Query);
};

}  // namespace codegen
}  // namespace peloton
