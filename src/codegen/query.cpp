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

#include "codegen/query.h"
#include "codegen/interpreter/context_builder.h"
#include "codegen/interpreter/query_interpreter.h"
#include "codegen/query_compiler.h"
#include "codegen/query_result_consumer.h"
#include "common/timer.h"
#include "executor/plan_executor.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace codegen {

// Constructor
Query::Query(const planner::AbstractPlan &query_plan)
    : query_plan_(query_plan) {}

void Query::Execute(std::unique_ptr<executor::ExecutorContext> executor_context,
                    QueryResultConsumer &consumer,
                    std::function<void(executor::ExecutionResult)> on_complete,
                    RuntimeStats *stats) {
  CodeGen codegen{GetCodeContext()};

  llvm::Type *runtime_state_type = runtime_state_.FinalizeType(codegen);
  size_t parameter_size = codegen.SizeOf(runtime_state_type);
  PL_ASSERT((parameter_size % 8 == 0) &&
            parameter_size >= sizeof(FunctionArguments) &&
            "parameter size not multiple of 8");

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  char *param = param_data.get();
  PL_MEMSET(param, 0, parameter_size);

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->storage_manager = storage::StorageManager::GetInstance();
  func_args->executor_context = executor_context.get();
  func_args->query_parameters = &executor_context->GetParams();
  func_args->consumer_arg = consumer.GetConsumerState();

  // bool force_interpreter =
  // settings::SettingsManager::GetBool(settings::SettingId::codegen_interpreter);
  bool force_interpreter = true;
  if (force_interpreter) {
    try {
      Interpret(func_args, stats);
    } catch (interpreter::NotSupportedException e) {
      // DEBUG
      LOG_INFO("query not supported by interpreter: %s", e.what());

      CompileAndExecute(func_args, stats);
    }
  } else {
    CompileAndExecute(func_args, stats);
  }

  executor::ExecutionResult result;
  result.m_result = ResultType::SUCCESS;
  result.m_processed = executor_context->num_processed;
  on_complete(result);
}

void Query::Prepare(const QueryFunctions &query_funcs) {
  query_funcs_ = query_funcs;

  // verify the functions
  // will also be done by Optimize() or Compile() if not done before,
  // but we do not want to mix up the timings, so do it here
  code_context_.Verify();

  // optimize the functions
  // TODO(marcel): add switch to enable/disable optimization
  // TODO(marcel): add timer to measure time used for optimization (see
  // RuntimeStats)
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

  // TODO(marcel): for now Compile() will always return true, find a way to
  // catch
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
  LOG_INFO("Using codegen interpreter to execute plan");

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  // Create Bytecode
  interpreter::InterpreterContext init_bytecode =
      interpreter::ContextBuilder::CreateInterpreterContext(
          code_context_, query_funcs_.init_func);
  interpreter::InterpreterContext plan_bytecode =
      interpreter::ContextBuilder::CreateInterpreterContext(
          code_context_, query_funcs_.plan_func);
  interpreter::InterpreterContext tear_down_bytecode =
      interpreter::ContextBuilder::CreateInterpreterContext(
          code_context_, query_funcs_.tear_down_func);

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->bytecode_compile_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    interpreter::QueryInterpreter::ExecuteFunction(
        init_bytecode, reinterpret_cast<char *>(function_arguments));
  } catch (...) {
    interpreter::QueryInterpreter::ExecuteFunction(
        tear_down_bytecode, reinterpret_cast<char *>(function_arguments));
    throw;
  }

  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    interpreter::QueryInterpreter::ExecuteFunction(
        plan_bytecode, reinterpret_cast<char *>(function_arguments));
  } catch (...) {
    interpreter::QueryInterpreter::ExecuteFunction(
        tear_down_bytecode, reinterpret_cast<char *>(function_arguments));
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
  interpreter::QueryInterpreter::ExecuteFunction(
      tear_down_bytecode, reinterpret_cast<char *>(function_arguments));

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
