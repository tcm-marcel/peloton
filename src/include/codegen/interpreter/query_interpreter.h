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

#pragma once

#include "codegen/query.h"
#include "codegen/interpreter/map_stack_frame.h"
#include "include/common/exception.h"

namespace peloton {

namespace test {
class InterpreterTest_ExecuteBranch_Test;
class InterpreterTest_ExecuteLoad_Test;
}

namespace codegen {
namespace interpreter {

class QueryInterpreter {
 public:
  // Change this line to change the stack frame implementation
  using StackFrame = MapStackFrame;

  // include value types
  using value_t = InterpreterUtils::value_t;
  using value_signed_t = InterpreterUtils::value_signed_t;

  // Holds the execution state for one function
  struct FunctionState {
    // Instruction pointer, updated by instruction execution functions
    llvm::Instruction *ip;

    // Frame holding llvm values and stack allocated memory
    StackFrame frame;

    // Pointer to the return value, set by the return instruction
    // can be nullptr if void function
    value_t return_value;
  };

 public:
  QueryInterpreter(const CodeContext &context);

  // Execute the query function (given by llvm::Function pointer) using the
  // query interpreter
  void ExecuteQueryFunction(llvm::Function *function,
                            Query::FunctionArguments *param);

  // Check if the interpreter is able to emulate all instructions used
  // in the function(s)
  static bool IsSupported(const Query::QueryFunctions &query_functions);
  static bool IsSupported(const llvm::Function *function);

 private:
  value_t CallFunction(llvm::Function *function, std::vector<value_t> params);

  // Dispatcher to the actual function which will execute the instruction
  void DispatchInstruction(const llvm::Instruction *instruction,
                           FunctionState &state);

  // Enter a basic block and process all PHI instructions at the beginning
  void EnterBasicBlock(llvm::BasicBlock *basic_block, FunctionState &state);

  static inline void IncreaseIp(QueryInterpreter::FunctionState &state)
      ALWAYS_INLINE {
    state.ip = state.ip->getNextNode();

    // only function that are not Terminators should call this function, so the
    // next IP should never be NULL
    PL_ASSERT(state.ip != nullptr);
  }

  value_t ExecuteLLVMInstrinsic(llvm::CallInst *instruction,
                                std::string intrinsic_name,
                                FunctionState &state);

  //--------------------------------------------------------------------------//
  // Instruction execution functions
  //
  // - the execution functions do not check integrity, the provided IR must
  //   have passed the verifier
  // - every instruction has to increment the instruction pointer (state.ip)
  //   if necessary
  // - use asserts to make sure that only instruction versions that are
  //   supported are actually executed TODO(marcel): check always?
  //--------------------------------------------------------------------------//

  // Terminators
  void ExecuteBranch(FunctionState &state);
  void ExecuteRet(FunctionState &state);

  // Standard binary operators
  void ExecuteBinaryIntegerOperator(FunctionState &state);
  void DispatchBinaryFloatingPointOperator(FunctionState &state);
  template <typename float_t>
  void ExecuteBinaryFloatingPointOperator(FunctionState &state);

  // Logical operators

  // Memory instructions
  void ExecuteLoad(FunctionState &state);
  void ExecuteAlloca(FunctionState &state);
  void ExecuteGetElementPtr(FunctionState &state);

  // Convert instructions
  void ExecuteBitCast(FunctionState &state);

  // Other instructions
  void ExecuteICmp(FunctionState &state);
  void ExecuteCall(FunctionState &state);
  void ExecuteSelect(FunctionState &state);
  void ExecuteSExt(FunctionState &state);

 private:
  // All test cases have to be added as class friends
  friend class peloton::test::InterpreterTest_ExecuteBranch_Test;
  friend class peloton::test::InterpreterTest_ExecuteLoad_Test;

  const CodeContext &context_;
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
