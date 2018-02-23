//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.cpp
//
// Identification: src/codegen/interpreter/query_interpreter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "codegen/interpreter/interpreter_context.h"
#include "codegen/interpreter/query_interpreter.h"

#include <cmath>


namespace peloton {
namespace codegen {
namespace interpreter {


#define INTERPRETER_DISPATCH_GOTO(ip) goto *(label_pointers_[InterpreterContext::GetOpcodeId(reinterpret_cast<const Instruction *>(ip)->op)])

void *QueryInterpreter::label_pointers_[InterpreterContext::GetNumberOpcodes()] = {nullptr};

QueryInterpreter::QueryInterpreter(const InterpreterContext &context)
    : context_(context) {}

value_t QueryInterpreter::ExecuteFunction(const InterpreterContext &context, const std::vector<value_t> arguments) {
  QueryInterpreter interpreter(context);
  interpreter.ExecuteFunction(arguments);

  return interpreter.GetReturnValue<value_t>();
}

void QueryInterpreter::ExecuteFunction(char *param) {
  ExecuteFunction({reinterpret_cast<value_t &>(param)});
}

__attribute__((__noinline__,__noclone__)) void QueryInterpreter::ExecuteFunction(std::vector<value_t> arguments) {
  if (label_pointers_[0] == nullptr) {

#define HANDLE_INST(op) label_pointers_[InterpreterContext::GetOpcodeId(Opcode::op)] = &&_ ## op;

#include "codegen/interpreter/bytecode_instructions.def"

  }

  printf("running interpreter:\n");
  InitializeActivationRecord(arguments);

  const Instruction *bytecode = reinterpret_cast<const Instruction *>(&context_.bytecode_[0]);
  const Instruction *ip = bytecode;

  // start with first instruction
  INTERPRETER_DISPATCH_GOTO(ip);

  //--------------------------------------------------------------------------//
  // Dispatch area (no actual loop, but labels and goto's)
  //--------------------------------------------------------------------------//

#ifndef NDEBUG
#define DEBUG_CODE_PRE \
  //LOG_DEBUG("%s", context_.Dump(ip).c_str())
#else
#define DEBUG_CODE_PRE
#endif

#define HANDLE_RET_INST(op) \
  _ret: \
    DEBUG_CODE_PRE; \
    GetValueReference<value_t>(0) = GetValue<value_t>(ip->args[0]); \
    return;

#define HANDLE_TYPED_INST(op, type) \
  _ ## op ## _ ## type: \
    DEBUG_CODE_PRE; \
    ip = op ## Handler<type>(ip); \
    INTERPRETER_DISPATCH_GOTO(ip);

#define HANDLE_INST(op) \
  _ ## op: \
    DEBUG_CODE_PRE; \
    ip = op ## Handler(ip); \
    INTERPRETER_DISPATCH_GOTO(ip);

#include "codegen/interpreter/bytecode_instructions.def"

  //--------------------------------------------------------------------------//
}

template <typename type_t>
type_t QueryInterpreter::GetReturnValue() {
  return GetValue<type_t>(0);
}

void QueryInterpreter::InitializeActivationRecord(std::vector<value_t> &arguments) {
  values_.resize(context_.number_values_);
  for (auto &constant : context_.constants_) {
    SetValue<value_t>(constant.second, constant.first);
  }

  if (context_.function_arguments_.size() != arguments.size()) {
    throw Exception("llvm function called through interpreter with wrong number of arguments");
  }

  for (unsigned int i = 0; i < context_.function_arguments_.size(); i++) {
    SetValue<value_t>(context_.function_arguments_[i], arguments[i]);
  }

  call_activations_.resize(context_.external_call_contexts_.size());
  for (size_t i = 0; i < context_.external_call_contexts_.size(); i++) {
    auto &call_context = context_.external_call_contexts_[i];
    auto &call_activation = call_activations_[i];

    if (ffi_prep_cif(&call_activation.call_interface, FFI_DEFAULT_ABI, call_context.args.size(),
                     call_context.dest_type, const_cast<ffi_type **>(call_context.arg_types.data())) != FFI_OK) {
      throw Exception("initializing ffi call interface failed ");
    }

    for (const auto &arg : call_context.args) {
      call_activation.value_pointers.push_back(&values_[arg]);
    }
    call_activation.return_pointer = &values_[call_context.dest_slot];
  }
}

uintptr_t QueryInterpreter::AllocateMemory(size_t number_bytes) {
  // allocate memory
  std::unique_ptr<char[]> pointer = std::make_unique<char[]>(number_bytes);

  // get raw pointer before moving pointer object!
  auto raw_pointer = reinterpret_cast<uintptr_t>(pointer.get());

  allocations_.emplace_back(std::move(pointer));
  return raw_pointer;
}


#ifndef NDEBUG
const llvm::Instruction *QueryInterpreter::GetIRInstructionFromIP(const Instruction *ip) {
  index_t index = context_.GetIndexFromIP(ip);
  return GetIRInstructionFromIP(static_cast<index_t>(index));
}

const llvm::Instruction *QueryInterpreter::GetIRInstructionFromIP(index_t instr_slot) {
  return context_.instruction_trace_.at(instr_slot);
}
#endif

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
