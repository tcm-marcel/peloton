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
  index_t interpreter_context;
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


class InterpreterContext {
 public:
  ALWAYS_INLINE inline static constexpr Opcode GetOpcodeFromId(index_t id) {
    return static_cast<Opcode>(id);
  }

  ALWAYS_INLINE inline static constexpr index_t GetOpcodeId(Opcode opcode) {
    return static_cast<index_t>(opcode);
  }

  static const char *GetOpcodeString(Opcode opcode);

  inline static constexpr size_t GetNumberOpcodes() {
    return static_cast<index_t>(Opcode::NUMBER_OPCODES);
  }

  ALWAYS_INLINE inline const Instruction *GetIPFromIndex(index_t index) const {
    return reinterpret_cast<const Instruction *>(const_cast<instr_slot_t *>(bytecode_.data()) + index);
  }

  ALWAYS_INLINE inline index_t GetIndexFromIP(const Instruction* instruction) const {
    index_t index = reinterpret_cast<const instr_slot_t *>(instruction) - bytecode_.data();
    return index;
  }

  static size_t GetInstructionSlotSize(const Instruction *instruction);

  static ALWAYS_INLINE inline size_t GetInteralCallInstructionSlotSize(const InternalCallInstruction *instruction) {
    return std::ceil<size_t>((4 + instruction->number_args) / sizeof(instr_slot_t));
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
  std::vector<llvm::Instruction *> instruction_trace_;
  #endif

 private:
  friend QueryInterpreter;
  friend ContextBuilder;

  // TODO(marcel) check non move/copy
  // This class cannot be copy or move-constructed
  //DISALLOW_COPY(InterpreterContext);
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
