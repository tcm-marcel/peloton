//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// context_builder.h
//
// Identification: src/include/codegen/interpreter/context_builder.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/interpreter/interpreter_context.h"

#include <vector>
#include <unordered_map>
#include <tuple>
#include <cmath>
#include <memory>
#include <cstdint>
#include <ffi.h>
#include <llvm/IR/Instructions.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/ADT/PostOrderIterator.h>

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {


class ContextBuilder {
 public:
  ContextBuilder(CodeContext &code_context, llvm::Function *function);

  InterpreterContext CreateInterpreterContext();

 private:
  typedef struct {
    index_t instruction_slot;
    index_t argument;
    llvm::BasicBlock *bb;
  } BytecodeRelocation;

  index_t AddConstant(llvm::Constant *constant);
  index_t AddConstant(int64_t value);
  index_t GetValueSlot(llvm::Value *value);
  void AddValueAlias(llvm::Value *value, llvm::Value *alias);
  index_t CreateAdditionalValueSlot();

  ffi_type *GetFFIType(llvm::Type *type);

  bool IsConstantValue(llvm::Value *value);
  int64_t GetConstantIntegerValueSigned(llvm::Value *constant);
  uint64_t GetConstantIntegerValueUnsigned(llvm::Value *constant);

  void RegisterAllocation();
  void TranslateFunction();

  Opcode GetOpcodeForTypeAllTypes(Opcode untyped_op, llvm::Type *type) const;
  Opcode GetOpcodeForTypeIntTypes(Opcode untyped_op, llvm::Type *type) const;
  Opcode GetOpcodeForTypeFloatTypes(Opcode untyped_op, llvm::Type *type) const;
  Opcode GetOpcodeForTypeSizeIntTypes(Opcode untyped_op, llvm::Type *type) const;

  template <size_t number_instruction_slots = 1>
  Instruction &InsertBytecodeInstruction(llvm::Instruction *llvm_instruction,
                                         Opcode opcode,
                                         index_t arg0 = 0,
                                         index_t arg1 = 0,
                                         index_t arg2 = 0,
                                         index_t arg3 = 0,
                                         index_t arg4 = 0,
                                         index_t arg5 = 0,
                                         index_t arg6 = 0);
  Instruction &InsertBytecodeInstruction(llvm::Instruction *llvm_instruction,
                                         Opcode opcode,
                                         llvm::Value *arg0,
                                         llvm::Value *arg1,
                                         llvm::Value *arg2);
  Instruction &InsertBytecodeInstruction(llvm::Instruction *llvm_instruction,
                                         Opcode opcode,
                                         llvm::Value *arg0,
                                         llvm::Value *arg1);
  ExternalCallInstruction &InsertBytecodeExternalCallInstruction(llvm::Instruction *llvm_instruction,
                                                                 index_t call_context,
                                                                 void *function);
  InternalCallInstruction &InsertBytecodeInternalCallInstruction(llvm::Instruction *llvm_instruction,
                                                                 index_t interpreter_context,
                                                                 index_t dest_slot,
                                                                 size_t number_arguments);

  bool IRLookAhead(llvm::Instruction *current_instruction, std::function<bool (llvm::Instruction *)> condition_check, llvm::Instruction *&next_instruction);

  void ProcessPHIsForBasicBlock(llvm::BasicBlock *bb);

  void TranslateBranch(llvm::Instruction *instruction);
  void TranslateReturn(llvm::Instruction *instruction);
  void TranslateBinaryOperator(llvm::Instruction *instruction);
  void TranslateAlloca(llvm::Instruction *instruction);
  void TranslateLoad(llvm::Instruction *instruction);
  void TranslateStore(llvm::Instruction *instruction);
  void TranslateGetElementPtr(llvm::Instruction *instruction);
  void TranslateIntExt(llvm::Instruction *instruction);
  void TranslateFloatIntCast(llvm::Instruction *instruction);
  void TranslateCmp(llvm::Instruction *instruction);
  void TranslateCall(llvm::Instruction *instruction);
  void TranslateSelect(llvm::Instruction *instruction);
  void TranslateExtractValue(llvm::Instruction *instruction);

 private:
  std::unordered_map<llvm::Value *, index_t> value_mapping_;

  std::unordered_map<llvm::BasicBlock *, index_t> bb_mapping_;
  std::vector<BytecodeRelocation> bytecode_relocations_;

  size_t number_values_;
  std::unordered_map<value_t, index_t> constants_;
  std::vector<instr_slot_t> bytecode_;
  std::vector<ExternalCallContext> external_call_contexts_;

  size_t number_arguments_;

  std::vector<InterpreterContext> sub_contexts_;
  std::unordered_map<llvm::Function *, index_t> sub_context_mapping_;

  #ifndef NDEBUG
  std::vector<llvm::Instruction *> instruction_trace_;
  #endif

  // initialization of ReversePostOrderTraversal is expensive!
  //llvm::ReversePostOrderTraversal<llvm::Function*> bb_traversal_;

  CodeContext &code_context_;
  llvm::Function *llvm_function_;
};

class NotSupportedException : public std::runtime_error {
 public:
  NotSupportedException(std::string message) : std::runtime_error(message) {}
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
