//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_context.h
//
// Identification: src/include/codegen/interpreter/interpreter_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/macros.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Type.h"

#include <vector>
#include <memory>
#include <unordered_map>
#include <cstdint>
#include <ffi.h>

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {

class QueryInterpreter;
class ContextBuilder;

// type definitions
using i8 = uint8_t;
using i16 = uint16_t;
using i32 = uint32_t;
using i64 = uint64_t;
using value_t = uint64_t;
using index_t = uint16_t;
using instr_slot_t = uint64_t;


enum class Opcode : index_t {
  undefined,

#define HANDLE_INST(opcode) opcode,
#include "codegen/interpreter/bytecode_instructions.def"
#undef HANDLE_INST

  NUMBER_OPCODES
};

typedef struct {
  Opcode op;
  index_t args[];
} Instruction;

typedef struct {
  Opcode op;
  index_t sub_context;
  index_t dest_slot;
  index_t number_args;
  index_t args[];
} InternalCallInstruction;

typedef struct {
  Opcode op;
  index_t external_call_context;
  void (*function)(void);
} ExternalCallInstruction;

typedef struct {
  index_t dest_slot;
  ffi_type *dest_type;
  std::vector<index_t> args;
  std::vector<ffi_type *> arg_types;
} ExternalCallContext;

/**
 * A InterpreterContext contains all information necessary to run a LLVM function in the interpreter and is completely independent from the CodeContext it was created from (except for the tracing information in debug mode). It can be moved and copied.
 */
class InterpreterContext {
 public:
  /**
   * Returns the Opcode enum for a given Opcode Id (to avoid plain casting)
   * @param id Opcode Id
   * @return Opcode enum
   */
  ALWAYS_INLINE inline static constexpr Opcode GetOpcodeFromId(index_t id) {
    return static_cast<Opcode>(id);
  }

  /**
   * Returns the Opcode Id to a given Opcode enum (to avoid plain casting)
   * @param opcode Opcode enum
   * @return Opcode Id
   */
  ALWAYS_INLINE inline static constexpr index_t GetOpcodeId(Opcode opcode) {
    return static_cast<index_t>(opcode);
  }

  /**
   * Returns a numan readable string to a given Opcode
   * @param opcode Opcode enum
   * @return String representation if the Opcode
   */
  static const char *GetOpcodeString(Opcode opcode);

  /**
   * Returns the overall number of existing Opcodes (not trivial, as the Opcodes are created with expanding macros)
   * @return overall number of existing Opcodes
   */
  inline static constexpr size_t GetNumberOpcodes() {
    return static_cast<index_t>(Opcode::NUMBER_OPCODES);
  }

  /**
   * Return the instruction pointer to a given instruction index (from this context)
   * @param index instruction index
   * @return pointer to the instruction at that index inside the bytecode
   */
  ALWAYS_INLINE inline const Instruction *GetIPFromIndex(index_t index) const {
    return reinterpret_cast<const Instruction *>(const_cast<instr_slot_t *>(bytecode_.data()) + index);
  }

  /**
   * Returns the instruction index for a given instruction pointer (from this context)
   * @param instruction pointer to a given instruction inside the bytecode
   * @return index to the instruction the pointer is pointing to
   */
  ALWAYS_INLINE inline index_t GetIndexFromIP(const Instruction* instruction) const {
    index_t index = reinterpret_cast<const instr_slot_t *>(instruction) - bytecode_.data();
    return index;
  }

  #ifndef NDEBUG
  const llvm::Instruction *GetIRInstructionFromIP(index_t instr_slot) const;
  #endif

  /**
   * Returns the number of slots a given instruction occupies in the bytecode stream
   * @param instruction pointer to the instruction inside the bytecode
   * @return number of slots (each 8 Byte) that are used by this instruction
   */
  static size_t GetInstructionSlotSize(const Instruction *instruction);

  static ALWAYS_INLINE inline size_t GetInteralCallInstructionSlotSize(const InternalCallInstruction *instruction) {
    const size_t number_slots = ((2 * (4 + instruction->number_args)) + sizeof(instr_slot_t) - 1) / sizeof(instr_slot_t);
    PL_ASSERT(number_slots > 0);
    return number_slots;
  }

  std::string DumpContents() const;
  std::string Dump(const Instruction *instruction) const;

 private:
  InterpreterContext() {}

 private:
  size_t number_values_;
  std::vector<std::pair<value_t, index_t> > constants_;
  std::vector<index_t> function_arguments_;
  std::vector<instr_slot_t> bytecode_;
  std::vector<ExternalCallContext> external_call_contexts_;

  std::vector<InterpreterContext> sub_contexts_;

  #ifndef NDEBUG
  std::vector<const llvm::Instruction *> instruction_trace_;
  #endif

 private:
  friend QueryInterpreter;
  friend ContextBuilder;
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
