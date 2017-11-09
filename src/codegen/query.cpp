//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query.cpp
//
// Identification: src/codegen/query.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/codegen/interpreter/query_interpreter.h>
#include "codegen/query.h"

#include "common/logger.h"
#include "common/timer.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

// Execute the query on the given database (and within the provided transaction)
// This really involves calling the init(), plan() and tearDown() functions, in
// that order. We also need to correctly handle cases where _any_ of those
// functions throw exceptions.
void Query::Execute(concurrency::Transaction &txn,
                    executor::ExecutorContext *executor_context,
                    char *consumer_arg, RuntimeStats *stats) {
  // Prepare runtime state
  CodeGen codegen{GetCodeContext()};

  // Calculate the size for the function arguments
  // The actual size can be grater than the sizeof(FunctionArguments)!
  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  uint64_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT(parameter_size % 8 == 0 &&
            parameter_size >= sizeof(FunctionArguments));

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};

  // Clean the space
  PL_MEMSET(param_data.get(), 0, parameter_size);

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->txn = &txn;
  func_args->catalog = storage::StorageManager::GetInstance();
  func_args->executor_context = executor_context;
  func_args->consumer_arg = consumer_arg;

  // TODO: decide dynamically what to use
  if (settings::SettingsManager::GetBool(
          settings::SettingId::codegen_interpreter)) {
    if (interpreter::QueryInterpreter::IsSupported(query_funcs_)) {
      Interpret(func_args, stats);
    } else {
      // Print hint
      LOG_INFO("CAUTION: interpretation forced but not query not supported!");
      CompileAndExecute(func_args, stats);
    }
  } else {
    CompileAndExecute(func_args, stats);
  }
}

void Query::Prepare(const QueryFunctions &query_funcs) {
  query_funcs_ = query_funcs;

  // verify the functions
  // will also be done by Optimize() or Compile() if not done before,
  // but we do not want to mix up the timings, so do it here
  code_context_.Verify();

  // optimize the functions
  // TODO: add switch to enable/disable optimization
  // TODO: add timer to measure time used for optimization (see RuntimeStats)
  code_context_.Optimize();
}

bool Query::CompileAndExecute(FunctionArguments *function_arguments,
                              RuntimeStats *stats) {
  // Timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  // Step 1: Compile all functions in context
  LOG_TRACE("Setting up Query ...");

  // TODO: for now Compile() will always return true, find a way to catch
  // compilation errors from LLVM
  if (!code_context_.Compile()) {
    return false;
  }

  // Get pointers to the JITed functions
  compiled_function_t init_func_ptr =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          query_funcs_.init_func);
  PL_ASSERT(init_func_ptr != nullptr);

  compiled_function_t plan_func_ptr =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          query_funcs_.plan_func);
  PL_ASSERT(plan_func_ptr != nullptr);

  compiled_function_t tear_down_func_ptr =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          query_funcs_.tear_down_func);
  PL_ASSERT(tear_down_func_ptr != nullptr);

  LOG_TRACE("Query has been setup ...");

  // Timer for JIT compilation
  if (stats != nullptr) {
    timer.Stop();
    stats->jit_compile_ms = timer.GetDuration();
    timer.Reset();
  }

  // Step 2: Execute query

  // Start timer
  if (stats != nullptr) {
    timer.Start();
  }

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    init_func_ptr(function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_ptr(function_arguments);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    plan_func_ptr(function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    tear_down_func_ptr(function_arguments);
    throw;
  }

  // Timer plan execution
  if (stats != nullptr) {
    timer.Stop();
    stats->plan_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Clean up
  LOG_TRACE("Calling query's tearDown() ...");
  tear_down_func_ptr(function_arguments);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }

  return true;
}

bool Query::Interpret(FunctionArguments *function_arguments,
                      RuntimeStats *stats) {
  interpreter::QueryInterpreter interpreter{code_context_};

  LOG_INFO("Using codegen interpreter to execute plan");

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    interpreter.ExecuteQueryFunction(query_funcs_.init_func,
                                     function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    interpreter.ExecuteQueryFunction(query_funcs_.tear_down_func,
                                     function_arguments);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    interpreter.ExecuteQueryFunction(query_funcs_.plan_func,
                                     function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    interpreter.ExecuteQueryFunction(query_funcs_.tear_down_func,
                                     function_arguments);
    throw;
  }

  // Timer plan execution
  if (stats != nullptr) {
    timer.Stop();
    stats->plan_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Clean up
  LOG_TRACE("Calling query's tearDown() ...");
  interpreter.ExecuteQueryFunction(query_funcs_.tear_down_func,
                                   function_arguments);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }

  // TODO(marcel): return value
  return true;
}

}  // namespace codegen
}  // namespace peloton
