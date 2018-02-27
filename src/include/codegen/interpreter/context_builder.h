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

#include <ffi.h>
#include <llvm/ADT/PostOrderIterator.h>
#include <llvm/IR/CFG.h>
#include <llvm/IR/Instructions.h>
#include <llvm/Support/raw_ostream.h>
#include <cmath>
#include <cstdint>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {

class ContextBuilder {
 public:
  /**
   * Static method to create a interpreter context from a code context.
   * @param code_context CodeContext containing the LLVM function
   * @param function LLVM function that shall be interpreted later
   * @return A InterpreterContext object that can be passed to the Query
   * Interpreter (several times).
   */
  static InterpreterContext CreateInterpreterContext(
      const CodeContext &code_context, const llvm::Function *function);

 private:
  // These types definitions have the purpose to make the code better
  // understandable. The context builder creates indexes to identify the
  // LLVM types, which usually are only accessed by raw pointers.
  // Those types shall indicate which index is meant by a function.
  // None of these indexes end up in the interpreter context!
  using value_index_t = index_t;
  using instruction_index_t = index_t;

  /**
   * Describes a bytecode relocation that has to be applied to add the
   * destination of a branch instruction once its value is available.
   * It gets created by TranslateBranch() and is processed after
   * TranslateFunction() processed all instructions.
   */
  typedef struct {
    index_t instruction_slot;
    index_t argument;
    const llvm::BasicBlock *bb;
  } BytecodeRelocation;

  /**
   * Describes the liveliness of a value (every value that has a value index),
   * defined by the definition and the last usage, both measured by the
   * continuous instruction index.
   */
  typedef struct ValueLiveliness {
    instruction_index_t definition;
    instruction_index_t last_usage;
  } Valueliveliness;

  /**
   * NULL-value for liveness information
   */
  static const index_t valueLivelinessUnknown =
      std::numeric_limits<index_t>::max();

 private:
  ContextBuilder(const CodeContext &code_context,
                 const llvm::Function *function);

  /**
   * Analyses the function to collect values and constants and gets
   * value liveliness information
   */
  void AnalyseFunction();

  /**
   * Naive register allocation that just assings a unique value slot to
   * every value
   */
  void PerformNaiveRegisterAllocation();

  /**
   * Greedy register allocation, that for each value tries to find the next free
   * value slot, that is not occupied anymore.
   */
  void PerformGreedyRegisterAllocation();

  /**
   * Helper function that ensures, that no values overlap in the same slot
   * in the value mapping created by the register allocation.
   */
  void ValidateRegisterMapping();

  /**
   * Dump the collected liveliness information in csv format
   * @return string with csv formatted values
   */
  std::string DumpValueInformation();

  /**
   * Translates all instructions into bytecode.
   */
  void TranslateFunction();

  /**
   * Do some final conversations to make the created InterpreterContext usable.
   */
  void Finalize();

 private:
  /**
   * Maps a given LLVM value to the same value index as another LLVM Value.
   * @param alias LLVM value
   * @param value_index the value index to map the LLVM value to. The
   * value index must already exist.
   * @return the value index that was given as parameter
   */
  value_index_t CreateValueAlias(const llvm::Value *alias,
                                 value_index_t value_index);
  /**
   * Create a unique value index for a given LLVM value. This function is meant
   * to be called only once per LLVM Value.
   * @param value LLVM Value
   * @return a new unique value index for this LLVM value
   */
  value_index_t CreateValueIndex(const llvm::Value *value);
  /**
   * Get the value index for a given LLVM value. The value index must have been
   * created with CreateValueIndex before.
   * @param value LLVM Value
   * @return the value index that is mapped to this LLVM value
   */
  value_index_t GetValueIndex(const llvm::Value *value) const;

  /**
   * Add a LLVM constant to this interpreter context. This function is meant
   * to be called once per LLVM Constant.
   * @param constant LLVM constant
   * @return a value index that refers to a constant with the same value. If
   * no internal constant with this value exists before, a new value index
   * is created.
   */
  value_index_t AddConstant(const llvm::Constant *constant);

  /**
   * Returns the value slot (register) for a given value index
   * @param value_index value index
   * @return the value slot (register) assigned by the register allocation
   */
  index_t GetValueSlot(value_index_t value_index) const;

  /**
   * Wrapper for GetValueSlot(value_index) that also does the index lookup
   */
  index_t GetValueSlot(const llvm::Value *value) const;

  /**
   * Adds a definition time to a values liveliness record. This function
   * is meant to be called only once per value. (cmp. SSA property)
   * @param value_index value index identifying the value
   * @param definition instruction index of the definition
   */
  void AddValueDefinition(value_index_t value_index,
                          instruction_index_t definition);

  /**
   * Adds a usage time to a values liveliness record. This function can be
   * called for every usage, the latest one will be kept.
   * @param value_index value index identifying the value
   * @param usage instruction index of the definition
   */
  void AddValueUsage(value_index_t value_index, instruction_index_t usage);

  /**
   * Returns the index for a additional temporary value slot in that
   * basic block. Due to the phi swap problem (lost copy) it can happen,
   * that during translation additional value slots are needed that have not
   * been mapped by the register allocation. The number of additional temporary
   * value slots is tracked and added to the overall number of value
   * slots during finalization.
   * @param bb basic block the temporary value slot shall be created in
   * @return a temporary value slot index, that can be used only in
   * this basic block
   */
  index_t GetTemporaryValueSlot(const llvm::BasicBlock *bb);

  /**
   * Returns the matching FFI type for a given LLVM type
   * @param type LLVM type
   * @return FFI type
   */
  ffi_type *GetFFIType(llvm::Type *type);

  /**
   * Checks if a LLVM Value is a constant
   * @param value LLVM Value
   * @return true, if the given LLVM value is a constant
   */
  bool IsConstantValue(const llvm::Value *value) const;

  /**
   * Extracts the actual constant value of a LLVM constant
   * @param constant LLVM constant
   * @return the actual value of the constant, sign or zero extended to
   * the size of value_t
   */
  value_t GetConstantValue(const llvm::Constant *constant) const;

  /**
   * Directly extracts the signed integer value of a integer constant
   * @param constant LLVM Constant that is a instance of llvm::ConstantInt
   * @return signed integer value of the LLVM constant
   */
  int64_t GetConstantIntegerValueSigned(llvm::Value *constant) const;

  /**
   * Directly extracts the unsigned integer value of a integer constant
   * @param constant LLVM Constant that is a instance of llvm::ConstantInt
   * @return unsigned integer value of the LLVM constant
   */
  uint64_t GetConstantIntegerValueUnsigned(llvm::Value *constant) const;

  /**
   * Checks if one basic block is the successor of another basic block
   * when walking all basic blocks in reverse post order.
   * (Because ->nextNode doesn't work then)
   * @param bb current LLVM basic block
   * @param succ LLVM basic block that shall be checked to be the successor
   * @return true, if succ is the successor of bb
   */
  bool BasicBlockIsRPOSucc(const llvm::BasicBlock *bb,
                           const llvm::BasicBlock *succ) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined for
   * _all_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_ALL_TYPES(op), where op must be defined for all types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeAllTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _integer_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_INT_TYPES(op), where op must be defined only for integer types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeIntTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _floating point_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_FLOAT_TYPES(op), where op must be defined only for float types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeFloatTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _integer_ types. In difference to the other function, this one only
   * considers the type size to determine the opcode type.
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_INT_TYPES(op), where op must be defined only for integer types.
   * @param type LLVM type to take the size information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeSizeIntTypes(Opcode untyped_op,
                                      llvm::Type *type) const;

  /**
   * Insert abytecode instruction with up to 6 arguments into the bytecode
   * stream.
   * @tparam number_instruction_slots Number of instruction slots this
   * instruction occupies (will be checked with number of arguments)
   * @param llvm_instruction LLVM function this instruction is created from.
   * (Only needed for tracing information, not used in Release mode!)
   * @param opcode Opcode of the bytecode instruction
   * @return Reference to the created instruction in the bytecode stream.
   */
  template <size_t number_instruction_slots = 1>
  Instruction &InsertBytecodeInstruction(
      const llvm::Instruction *llvm_instruction, Opcode opcode,
      index_t arg0 = 0, index_t arg1 = 0, index_t arg2 = 0, index_t arg3 = 0,
      index_t arg4 = 0, index_t arg5 = 0, index_t arg6 = 0);

  /**
   * Insert bytecode instruction with 3 arguments into the bytecode stream.
   * Wrapper that automatically gets the value slots for the LLVM values
   * provided.
   */
  Instruction &InsertBytecodeInstruction(
      const llvm::Instruction *llvm_instruction, Opcode opcode,
      const llvm::Value *arg0, const llvm::Value *arg1,
      const llvm::Value *arg2);

  /**
   * Insert bytecode instruction with 2 arguments into the bytecode stream.
   * Wrapper that automatically gets the value slots for the LLVM values
   * provided.
   */
  Instruction &InsertBytecodeInstruction(
      const llvm::Instruction *llvm_instruction, Opcode opcode,
      const llvm::Value *arg0, const llvm::Value *arg1);

  /**
   * Insert a external call bytecode instruction into the bytecode stream.
   * @param llvm_instruction LLVM function this instruction is created from.
   * (Only needed for tracing information, not used in Release mode!)
   * @param call_context index of the call context created for this external
   * call instruction
   * @param function function pointer to the external function
   * @return Reference to the created instruction in the bytecode stream.
   */
  ExternalCallInstruction &InsertBytecodeExternalCallInstruction(
      const llvm::Instruction *llvm_instruction, index_t call_context,
      void *function);

  /**
   * Insert a internal call bytecode instruction into the bytecode stream.
   * @param llvm_instruction LLVM function this instruction is created from.
   * (Only needed for tracing information, not used in Release mode!)
   * @param sub_context index to the sub context (interpreter context) created
   * for this internal call instruction
   * @param dest_slot Destination slot for the return value. Set zero if
   * internal function returns void.
   * @param number_arguments number of arguments provided in this function call.
   * The internal call instruction has variadic size, depending on the number
   * of arguments!
   * @return Reference to the created instruction in the bytecode stream.
   */
  InternalCallInstruction &InsertBytecodeInternalCallInstruction(
      const llvm::Instruction *llvm_instruction, index_t sub_context,
      index_t dest_slot, size_t number_arguments);

  /**
   * Resolves the PHI nodes referring to this basic block, by placing mov
   * instructions. Must be called just before the terminating LLVM instruction
   * in basic block. If the PHI swap / lost copy problem can occur, the
   * function creates additional mov instructions.
   * @param bb current basic block
   */
  void ProcessPHIsForBasicBlock(const llvm::BasicBlock *bb);

  // Following functions get called by the

  void TranslateBranch(const llvm::Instruction *instruction,
                       std::vector<BytecodeRelocation> &bytecode_relocations);
  void TranslateReturn(const llvm::Instruction *instruction);
  void TranslateBinaryOperator(const llvm::Instruction *instruction);
  void TranslateAlloca(const llvm::Instruction *instruction);
  void TranslateLoad(const llvm::Instruction *instruction);
  void TranslateStore(const llvm::Instruction *instruction);
  void TranslateGetElementPtr(const llvm::Instruction *instruction);
  void TranslateIntExt(const llvm::Instruction *instruction);
  void TranslateFloatIntCast(const llvm::Instruction *instruction);
  void TranslateCmp(const llvm::Instruction *instruction);
  void TranslateCall(const llvm::Instruction *instruction);
  void TranslateSelect(const llvm::Instruction *instruction);
  void TranslateExtractValue(const llvm::Instruction *instruction);

 private:
  /**
   *  The interpreter context that is created (and then moved). All other
   * members are helping data structures that don't end up in the resulting
   * context
   */
  InterpreterContext context_;

  /**
   * Mapping from Value* to internal value index (includes merged
   * values/constants). Value index is used to access the vectors below.
   */
  std::unordered_map<const llvm::Value *, value_index_t> value_mapping_;

  /**
   * Holds the liveliness of each value (use/def)
   */
  std::vector<ValueLiveliness> value_liveliness_;

  /**
   * Holds the assigned value slot (from register allocation)
   */
  std::vector<index_t> value_slots_;

  /**
   * Overall number of value slots needed (from register allocation)
   * without temporary value slots (added during translation)
   */
  size_t number_value_slots_;

  /**
   * Holds all constants, their actual values and their value index
   */
  std::vector<std::pair<value_t, value_index_t>> constants_;

  /**
   * Maximum instruction index = highest possible liveliness value
   */
  instruction_index_t instruction_index_max_;

  /**
   * Additional temporary value slots (created due to phi swap problem).
   * Mapping from instruction index to number of temporary slots needed
   * at that time (specified by instruction index).
   */
  std::unordered_map<const llvm::BasicBlock *, index_t>
      number_temporary_values_;

  /**
   * Maximum number of temporary value slots needed at all time points.
   */
  size_t number_temporary_value_slots_;

  /**
   * Keep track of all Call instructions that refer to a overflow aware
   * operation, as their results get directly saved in the destination slots
   * of the ExtractValue instructions refering to them.
   */
  std::unordered_map<
      const llvm::CallInst *,
      std::pair<const llvm::ExtractValueInst *, const llvm::ExtractValueInst *>>
      overflow_results_mapping_;

  /**
   * Mapping of functions to subcontexts to avoid duplicated contexts
   * if a internal function is called several times
   */
  std::unordered_map<const llvm::Function *, index_t> sub_context_mapping_;

  /**
   * ReversePostOrderTraversal, which is used for all BB traversals
   * Initialization is very expensive, so we reuse it
   * cannot be const, because the class doesn't provide const iterators
   */
  llvm::ReversePostOrderTraversal<const llvm::Function *> rpo_traversal_;

  /**
   * A vector holding the the basic block pointers in reverse post order.
   * This vector is retrieved from the RPO traversal and necessary
   * to make pred/pred lookups.
   */
  std::vector<const llvm::BasicBlock *> bb_reverse_post_order_;

  /**
   * Original code context the context is build from
   */
  const CodeContext &code_context_;

  /**
   * LLVM function that shall be translated
   */
  const llvm::Function *llvm_function_;
};

class NotSupportedException : public std::runtime_error {
 public:
  NotSupportedException(std::string message) : std::runtime_error(message) {}
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
