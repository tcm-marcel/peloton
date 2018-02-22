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
#include <llvm/IR/CFG.h>
#include <llvm/ADT/PostOrderIterator.h>

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {


class ContextBuilder {
 public:
  static InterpreterContext CreateInterpreterContext(CodeContext &code_context, llvm::Function *function);

 private:
  // These types just have the purpose to make the code better understandable.
  // They are used to map ascending IDs to the LLVM types, which usually are
  // only accessed by raw pointers.
  using value_index_t = index_t;
  using constant_index_t = index_t;
  using instruction_index_t = index_t;

  typedef struct {
    index_t instruction_slot;
    index_t argument;
    llvm::BasicBlock *bb;
  } BytecodeRelocation;

  static const index_t valueLivenessUnknown = std::numeric_limits<index_t>::max();

  typedef struct ValueLiveness {
    index_t definition;
    index_t last_usage;
  } ValueLiveness;

 private:
  ContextBuilder(CodeContext &code_context, llvm::Function *function);
  void AnalyseFunction();
  void PerformNaiveRegisterAllocation();
  void PerformGreedyRegisterAllocation();
  void TranslateFunction();
  void Finalize();

 private:
  value_index_t CreateValueAlias(llvm::Value *value, value_index_t value_alias_index);
  value_index_t CreateValueIndex(llvm::Value *value);
  value_index_t GetValueIndex(llvm::Value *value) const;

  value_index_t AddConstant(llvm::Constant *constant);
  index_t GetValueSlot(value_index_t value_index) const;
  index_t GetValueSlot(llvm::Value *value) const;
  void AddValueDefinition(value_index_t value_index, instruction_index_t definition);
  void AddValueUsage(value_index_t value_index, instruction_index_t usage);
  index_t GetTemporaryValueSlot(llvm::BasicBlock *bb);

  ffi_type *GetFFIType(llvm::Type *type);

  bool IsConstantValue(llvm::Value *value) const;
  value_t GetConstantValue(llvm::Constant *constant) const;
  int64_t GetConstantIntegerValueSigned(llvm::Value *constant) const;
  uint64_t GetConstantIntegerValueUnsigned(llvm::Value *constant) const;

  bool BasicBlockIsRPOPred(llvm::BasicBlock *bb, llvm::BasicBlock *pred) const;
  bool BasicBlockIsRPOSucc(llvm::BasicBlock *bb, llvm::BasicBlock *succ) const;

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

  void ProcessPHIsForBasicBlock(llvm::BasicBlock *bb);

  void TranslateBranch(llvm::Instruction *instruction, std::vector<BytecodeRelocation> &bytecode_relocations);
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
  /**
   *  The interpreter context that is created (and then moved). All other members are helping data structures that don't end up in the resulting context
   */
  InterpreterContext context_;

  /**
   * Mapping from Value* to internal value index (includes merged values/constants). Value index is used to access the vectors below.
   */
  std::unordered_map<llvm::Value *, value_index_t> value_mapping_;

  /**
   * Holds the liveness of each value (use/def)
   */
  std::vector<ValueLiveness> value_liveness_;

  /**
   * Holds the assigned value slot (from register allocation)
   */
  std::vector<index_t> value_slots_;

  /**
   * Overall number of value slots needed (from register allocation)
   */
  size_t number_value_slots_;

  /**
   * Holds all constants as values and their value index
   */
  std::vector<std::pair<value_t, value_index_t>> constants_;

  /**
   * Maximum instruction index = highest possible liveness value
   */
  instruction_index_t instruction_index_max_;

  /**
   * Additional temporary value slots (created due to phi swap problem).
   * Mapping from instruction index to number of temporary slots needed
   * at that time.
   */
  std::unordered_map<llvm::BasicBlock *, index_t> number_temporary_values_;

  /**
   * Maximum number of temporary value slots needed at all time points.
   */
  size_t number_temporary_value_slots_;

  /**
   * Mapping of functions to subcontexts to avoid duplicated contexts
   * if a internal function is called several times
   */
  std::unordered_map<llvm::Function *, index_t> sub_context_mapping_;

  /**
   * ReversePostOrderTraversal, which is used for all BB traversals
   * Initialization is very expensive, so we reuse it
   * cannot be const, because the class doesn't provide const iterators
   */
  llvm::ReversePostOrderTraversal<llvm::Function*> rpo_traversal_;

  /**
   * A vector holding the the basic block pointers in reverse post order.
   * This vector is retrieved from the RPO traversal and necessary
   * to make pred/pred lookups.
   */
  std::vector<llvm::BasicBlock *> bb_reverse_post_order_;

  /**
   * Original code context the context is build from
   */
  CodeContext &code_context_;

  /**
   * LLVM function that shall be translated
   */
  llvm::Function *llvm_function_;
};

class NotSupportedException : public std::runtime_error {
 public:
  NotSupportedException(std::string message) : std::runtime_error(message) {}
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
