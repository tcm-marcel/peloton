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

#include "codegen/interpreter/query_interpreter.h"
#include "codegen/interpreter/interpreter_context.h"

namespace peloton {
namespace codegen {
namespace interpreter {

/** This is the actual dispatch code: It lookups the destination handler address
 *  in the label_pointers_ array and performs a direct jump there.
 */
#define INTERPRETER_DISPATCH_GOTO(ip)                     \
  goto *(label_pointers_[InterpreterContext::GetOpcodeId( \
      reinterpret_cast<const Instruction *>(ip)->op)])

/**
 * The array with the label pointers has to be zero initialized to make sure,
 * that we fill it with the actual values on the first execution.
 */
void
    *QueryInterpreter::label_pointers_[InterpreterContext::GetNumberOpcodes()] =
        {nullptr};

QueryInterpreter::QueryInterpreter(const InterpreterContext &context)
    : context_(context) {}

value_t QueryInterpreter::ExecuteFunction(
    const InterpreterContext &context, const std::vector<value_t> &arguments) {
  QueryInterpreter interpreter(context);
  interpreter.ExecuteFunction(arguments);

  return interpreter.GetReturnValue<value_t>();
}

void QueryInterpreter::ExecuteFunction(const InterpreterContext &context,
                                       char *param) {
  QueryInterpreter interpreter(context);
  interpreter.ExecuteFunction({reinterpret_cast<value_t &>(param)});
}

__attribute__((__noinline__, __noclone__)) void
QueryInterpreter::ExecuteFunction(const std::vector<value_t> &arguments) {
  // Fill the value_pointers_ array with the handler addresses at first
  // startup. (This can't be done outside of this function, as the labels are
  // not visible there.
  if (label_pointers_[0] == nullptr) {
#define HANDLE_INST(op) \
  label_pointers_[InterpreterContext::GetOpcodeId(Opcode::op)] = &&_##op;

#include "codegen/interpreter/bytecode_instructions.def"
  }

  InitializeActivationRecord(arguments);

  // Get initial instruction pointer
  const Instruction *bytecode =
      reinterpret_cast<const Instruction *>(&context_.bytecode_[0]);
  const Instruction *ip = bytecode;

  // Start execution with first instruction
  INTERPRETER_DISPATCH_GOTO(ip);

//--------------------------------------------------------------------------//
//                             Dispatch area
//
// This is the actual dispatch area of the interpreter. Because we use
// threaded interpretation, this is not a dispatch loop, but a long list of
// labels, and the control flow jumps from one handler to the next with
// goto's -> INTERPRETER_DISPATCH_GOTO(ip)
//
// The whole dispatch area gets generated using the bytecode_instructions.def
// file. All instruction handlers from query_interpreter.h will get inlined
// here for all their types. Even though the function looks small here,
// it will be over 13kB in the resulting binary!
//--------------------------------------------------------------------------//

#ifndef NDEBUG
#define DEBUG_CODE_PRE LOG_TRACE("%s", context_.Dump(ip).c_str())
#else
#define DEBUG_CODE_PRE
#endif

#define HANDLE_RET_INST(op)                                       \
  _ret:                                                           \
  DEBUG_CODE_PRE;                                                 \
  GetValueReference<value_t>(0) = GetValue<value_t>(ip->args[0]); \
  return;

#define HANDLE_TYPED_INST(op, type) \
  _##op##_##type : DEBUG_CODE_PRE;  \
  ip = op##Handler<type>(ip);       \
  INTERPRETER_DISPATCH_GOTO(ip);

#define HANDLE_INST(op)   \
  _##op : DEBUG_CODE_PRE; \
  ip = op##Handler(ip);   \
  INTERPRETER_DISPATCH_GOTO(ip);

#include "codegen/interpreter/bytecode_instructions.def"

  //--------------------------------------------------------------------------//
}

template <typename type_t>
type_t QueryInterpreter::GetReturnValue() {
  // the ret instruction saves the return value in value slot 0 by definition
  return GetValue<type_t>(0);
}

void QueryInterpreter::InitializeActivationRecord(
    const std::vector<value_t> &arguments) {
  // resize vector to required number of value slots
  values_.resize(context_.number_values_);

  // fill in constants
  for (auto &constant : context_.constants_) {
    SetValue<value_t>(constant.second, constant.first);
  }

  // check if provided number or arguments matches the number required by
  // the function
  if (context_.function_arguments_.size() != arguments.size()) {
    throw Exception(
        "llvm function called through interpreter with wrong number of "
        "arguments");
  }

  // fill in function arguments
  for (unsigned int i = 0; i < context_.function_arguments_.size(); i++) {
    SetValue<value_t>(context_.function_arguments_[i], arguments[i]);
  }

  // prepare call activations
  call_activations_.resize(context_.external_call_contexts_.size());
  for (size_t i = 0; i < context_.external_call_contexts_.size(); i++) {
    auto &call_context = context_.external_call_contexts_[i];
    auto &call_activation = call_activations_[i];

    // initialize libffi call interface
    if (ffi_prep_cif(&call_activation.call_interface, FFI_DEFAULT_ABI,
                     call_context.args.size(), call_context.dest_type,
                     const_cast<ffi_type **>(call_context.arg_types.data())) !=
        FFI_OK) {
      throw Exception("initializing ffi call interface failed ");
    }

    // save the pointers to the value slots in the continuous arrays
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

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
