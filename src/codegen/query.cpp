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
#include "codegen/interpreter/bytecode_builder.h"
#include "codegen/interpreter/bytecode_interpreter.h"
#include "codegen/query_compiler.h"
#include "codegen/query_result_consumer.h"
#include "common/timer.h"
#include "executor/plan_executor.h"
#include "settings/settings_manager.h"

#include "common/benchmark.h"

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
  PELOTON_ASSERT((parameter_size % 8 == 0) &&
            parameter_size >= sizeof(FunctionArguments) &&
            "parameter size not multiple of 8");

  // Allocate some space for the function arguments
  std::unique_ptr<char[]> param_data{new char[parameter_size]};
  char *param = param_data.get();
  PELOTON_MEMSET(param, 0, parameter_size);

  // Set up the function arguments
  auto *func_args = reinterpret_cast<FunctionArguments *>(param_data.get());
  func_args->storage_manager = storage::StorageManager::GetInstance();
  func_args->executor_context = executor_context.get();
  func_args->query_parameters = &executor_context->GetParams();
  func_args->consumer_arg = consumer.GetConsumerState();

  Benchmark::Start(1, "query execute");

  if (Benchmark::execution_method_ == Benchmark::ExecutionMethod::LLVMInterpreter)
    ExecuteInterpreter(func_args, stats);
  else if (Benchmark::execution_method_ == Benchmark::ExecutionMethod::LLVMNative) {
    ExecuteNative(func_args, stats);
  } else if (Benchmark::execution_method_ == Benchmark::ExecutionMethod::Adaptive) {
    if (is_compiled_) {
      ExecuteNative(func_args, stats);
    } else {
      try {
        ExecuteInterpreter(func_args, stats);
      } catch (interpreter::NotSupportedException e) {
        LOG_ERROR("query not supported by interpreter: %s", e.what());

        executor::ExecutionResult result;
        result.m_result = ResultType::INVALID;
        on_complete(result);
        return;
      }
    }
  }

  Benchmark::Stop(1, "query execute");

  executor::ExecutionResult result;
  result.m_result = ResultType::SUCCESS;
  result.m_processed = executor_context->num_processed;
  on_complete(result);
}

void Query::Prepare(const LLVMFunctions &query_funcs) {
  llvm_functions_ = query_funcs;

  // verify the functions
  // will also be done by Optimize() or Compile() if not done before,
  // but we do not want to mix up the timings, so do it here

  code_context_.Verify();

  // optimize the functions
  // TODO(marcel): add switch to enable/disable optimization
  // TODO(marcel): add timer to measure time used for optimization (see
  // RuntimeStats)
  Benchmark::Start(1, "llvm optimize");
  code_context_.Optimize();
  Benchmark::Stop(1, "llvm optimize");

  is_compiled_ = false;
}

void Query::Compile(CompileStats *stats) {
  // Timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  Benchmark::Start(1, "llvm compile");

  // Compile all functions in context
  LOG_TRACE("Starting Query compilation ...");
  code_context_.Compile();

  Benchmark::Stop(1, "llvm compile");

  // Get pointers to the JITed functions
  compiled_functions_.init_func =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          llvm_functions_.init_func);
  PELOTON_ASSERT(compiled_functions_.init_func != nullptr);

  compiled_functions_.plan_func =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          llvm_functions_.plan_func);
  PELOTON_ASSERT(compiled_functions_.plan_func != nullptr);

  compiled_functions_.tear_down_func =
      (compiled_function_t)code_context_.GetRawFunctionPointer(
          llvm_functions_.tear_down_func);
  PELOTON_ASSERT(compiled_functions_.tear_down_func != nullptr);

  is_compiled_ = true;

  LOG_TRACE("Compilation finished.");

  // Timer for JIT compilation
  if (stats != nullptr) {
    timer.Stop();
    stats->compile_ms = timer.GetDuration();
    timer.Reset();
  }
}

bool Query::ExecuteNative(FunctionArguments *function_arguments,
                          RuntimeStats *stats) {
  Compile();

  // Start timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  Benchmark::Start(1, "native execute init");

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    compiled_functions_.init_func(function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    compiled_functions_.tear_down_func(function_arguments);
    throw;
  }

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  Benchmark::Stop(1, "native execute init");
  Benchmark::Start(1, "native execute plan");

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    compiled_functions_.plan_func(function_arguments);
  } catch (...) {
    // Cleanup if an exception is encountered
    compiled_functions_.tear_down_func(function_arguments);
    throw;
  }

  // Timer plan execution
  if (stats != nullptr) {
    timer.Stop();
    stats->plan_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  Benchmark::Stop(1, "native execute plan");
  Benchmark::Start(1, "native execute teardown");

  // Clean up
  LOG_TRACE("Calling query's tearDown() ...");
  compiled_functions_.tear_down_func(function_arguments);

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }

  Benchmark::Stop(1, "native execute teardown");

  return true;
}

bool Query::ExecuteInterpreter(FunctionArguments *function_arguments,
                               RuntimeStats *stats) {
  LOG_INFO("Using codegen interpreter to execute plan");

  // Timer
  Timer<std::ratio<1, 1000>> timer;
  if (stats != nullptr) {
    timer.Start();
  }

  Benchmark::Start(1, "interpreter translate init");

  // Create Bytecode
  interpreter::BytecodeFunction init_bytecode =
      interpreter::BytecodeBuilder::CreateBytecodeFunction(
          code_context_, llvm_functions_.init_func);

  Benchmark::Stop(1, "interpreter translate init");
  Benchmark::Start(1, "interpreter translate plan");

  Benchmark::Activate(2);
  interpreter::BytecodeFunction plan_bytecode =
      interpreter::BytecodeBuilder::CreateBytecodeFunction(
          code_context_, llvm_functions_.plan_func);
  Benchmark::Deactivate(2);

  Benchmark::Stop(1, "interpreter translate plan");
  Benchmark::Start(1, "interpreter translate teardown");

  interpreter::BytecodeFunction tear_down_bytecode =
      interpreter::BytecodeBuilder::CreateBytecodeFunction(
          code_context_, llvm_functions_.tear_down_func);

  Benchmark::Stop(1, "interpreter translate teardown");

  // Time initialization
  if (stats != nullptr) {
    timer.Stop();
    stats->interpreter_prepare_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  Benchmark::Start(1, "interpreter execute init");

  // Call init
  LOG_TRACE("Calling query's init() ...");
  try {
    interpreter::BytecodeInterpreter::ExecuteFunction(
        init_bytecode, reinterpret_cast<char *>(function_arguments));
  } catch (...) {
    interpreter::BytecodeInterpreter::ExecuteFunction(
        tear_down_bytecode, reinterpret_cast<char *>(function_arguments));
    throw;
  }

  if (stats != nullptr) {
    timer.Stop();
    stats->init_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  Benchmark::Stop(1, "interpreter execute init");
  Benchmark::Start(1, "interpreter execute plan");

  // Execute the query!
  LOG_TRACE("Calling query's plan() ...");
  try {
    Benchmark::Activate(2);
    interpreter::BytecodeInterpreter::ExecuteFunction(
        plan_bytecode, reinterpret_cast<char *>(function_arguments));
    Benchmark::Deactivate(2);
  } catch (...) {
    interpreter::BytecodeInterpreter::ExecuteFunction(
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

  Benchmark::Stop(1, "interpreter execute plan");
  Benchmark::Start(1, "interpreter execute teardown");

  // Clean up
  LOG_TRACE("Calling query's tearDown() ...");
  interpreter::BytecodeInterpreter::ExecuteFunction(
      tear_down_bytecode, reinterpret_cast<char *>(function_arguments));

  // No need to cleanup if we get an exception while cleaning up...
  if (stats != nullptr) {
    timer.Stop();
    stats->tear_down_ms = timer.GetDuration();
  }

  Benchmark::Stop(1, "interpreter execute teardown");

  // TODO(marcel): return value
  return true;
}

}  // namespace codegen
}  // namespace peloton
