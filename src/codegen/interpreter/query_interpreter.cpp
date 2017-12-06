//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.cpp
//
// Identification: src/codegen/interpreter/query_interpreter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/query_interpreter.h"

#include <ffi.h>
#include <include/common/exception.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/Support/raw_ostream.h>
#include <sstream>
#include <string>

namespace peloton {
namespace codegen {
namespace interpreter {

QueryInterpreter::QueryInterpreter(const CodeContext &context)
    : context_(context) {}

void QueryInterpreter::ExecuteQueryFunction(llvm::Function *function,
                                            Query::FunctionArguments *param) {
  CallFunction(function, {reinterpret_cast<value_t>(param)});
}

bool QueryInterpreter::IsSupported(
    const Query::QueryFunctions &query_functions) {
  if (!IsSupported(query_functions.init_func)) return false;
  if (!IsSupported(query_functions.plan_func)) return false;
  if (!IsSupported(query_functions.tear_down_func)) return false;

  return true;
}

bool QueryInterpreter::IsSupported(const llvm::Function *function) {
  for (llvm::const_inst_iterator instruction = llvm::inst_begin(function),
                                 end = llvm::inst_end(function);
       instruction != end; ++instruction) {
    switch (instruction->getOpcode()) {
      case llvm::Instruction::Br:
      case llvm::Instruction::Ret:
      // Standard binary operators
      case llvm::Instruction::Add:
      case llvm::Instruction::Sub:
      case llvm::Instruction::Mul:
      case llvm::Instruction::UDiv:
      case llvm::Instruction::SDiv:
      case llvm::Instruction::URem:
      case llvm::Instruction::SRem:
      case llvm::Instruction::FAdd:
      case llvm::Instruction::FSub:
      case llvm::Instruction::FMul:
      case llvm::Instruction::FDiv:
      case llvm::Instruction::FRem:
      // Logical operators
      // Memory instructions
      case llvm::Instruction::Load:
      case llvm::Instruction::Alloca:
      case llvm::Instruction::GetElementPtr:
      // Convert instructions
      case llvm::Instruction::BitCast:
      // Other instructions
      case llvm::Instruction::ICmp:
      case llvm::Instruction::Call:
      case llvm::Instruction::Select:
      case llvm::Instruction::PHI:
      case llvm::Instruction::SExt:
        continue;

      default: {
        LOG_DEBUG("Instruction %s not supported from interpreter",
                  instruction->getOpcodeName());
        return false;
      }
    }
  }

  return true;
}

QueryInterpreter::value_t QueryInterpreter::CallFunction(
    llvm::Function *function, std::vector<value_t> params) {
  // Get first instruction in first basic block
  llvm::Instruction *initial_ip = function->getEntryBlock().getFirstNonPHI();
  PL_ASSERT(initial_ip != nullptr);

  // Create state for this function
  FunctionState state{initial_ip, StackFrame{context_}, 0};

  // Make sure that the correct size of arguments is provided
  PL_ASSERT(function->getFunctionType()->getNumParams() == params.size());

  // Perpare function arguments
  auto *current_argument = &function->getArgumentList().front();
  for (auto param : params) {
    // add the value to the new stack frame
    state.frame.SetValue(current_argument, param);
    current_argument = current_argument->getNextNode();
  }

  // execute instructions until a Terminator instruction sets state.ip to NULL,
  // indicating that the function should return
  while (state.ip != nullptr) {
    LOG_TRACE("\nInterpreter will execute: %s (yields %s)\n ",
              InterpreterUtils::Print(state.ip).c_str(),
              InterpreterUtils::Print(state.ip->getType()).c_str());

    DispatchInstruction(state.ip, state);
  }

  return state.return_value;
}

void QueryInterpreter::DispatchInstruction(const llvm::Instruction *instruction,
                                           FunctionState &frame) {
  switch (instruction->getOpcode()) {
    // Terminators
    case llvm::Instruction::Br:
      ExecuteBranch(frame);
      break;

    case llvm::Instruction::Ret:
      ExecuteRet(frame);
      break;

    // Standard binary operators
    case llvm::Instruction::Add:
    case llvm::Instruction::Sub:
    case llvm::Instruction::Mul:
    case llvm::Instruction::UDiv:
    case llvm::Instruction::SDiv:
    case llvm::Instruction::URem:
    case llvm::Instruction::SRem:
      ExecuteBinaryIntegerOperator(frame);
      break;

    case llvm::Instruction::FAdd:
    case llvm::Instruction::FSub:
    case llvm::Instruction::FMul:
    case llvm::Instruction::FDiv:
    case llvm::Instruction::FRem:
      DispatchBinaryFloatingPointOperator(frame);
      break;

    // Logical operators

    // Memory instructions
    case llvm::Instruction::Load:
      ExecuteLoad(frame);
      break;

    case llvm::Instruction::Alloca:
      ExecuteAlloca(frame);
      break;

    case llvm::Instruction::GetElementPtr:
      ExecuteGetElementPtr(frame);
      break;

    // Convert instructions
    case llvm::Instruction::BitCast:
      ExecuteBitCast(frame);
      break;

    // Other instructions
    case llvm::Instruction::ICmp:
      ExecuteICmp(frame);
      break;

    case llvm::Instruction::Call:
      ExecuteCall(frame);
      break;

    case llvm::Instruction::Select:
      ExecuteSelect(frame);
      break;

    case llvm::Instruction::SExt:
      ExecuteSExt(frame);
      break;

    // Instruction is not supported
    default: {
      // If the function passed IsSupported() there might be an inconsistency
      throw Exception(
          "QueryInterpreter encountered unsupported llvm instruction: " +
          InterpreterUtils::Print(instruction));
    }
  }
}

// Enter a basic block and process all PHI instructions at the beginning
void QueryInterpreter::EnterBasicBlock(llvm::BasicBlock *basic_block,
                                       FunctionState &state) {
  // This function must not be called for the entry block of a function!
  // Entry blocks cannot contain any PHI nodes
  PL_ASSERT(state.ip != nullptr);

  // get last basic block
  llvm::BasicBlock *last_bb = state.ip->getParent();

  // iterate PHI nodes
  for (auto *instruction = &basic_block->front();
       instruction->getOpcode() == llvm::Instruction::PHI;
       instruction = instruction->getNextNode()) {
    auto *phi = llvm::cast<llvm::PHINode>(instruction);

    // get value for last basic block and save with new value pointer
    value_t value =
        state.frame.GetValue(phi->getIncomingValueForBlock(last_bb));
    state.frame.SetValue(phi, value);
  }

  state.ip = basic_block->getFirstNonPHI();
}

InterpreterUtils::value_t QueryInterpreter::ExecuteLLVMInstrinsic(
    llvm::CallInst *instruction, std::string intrinsic_name,
    FunctionState &state) {
  if (intrinsic_name.substr(0, 11) == "llvm.memcpy") {
    PL_ASSERT(instruction->getNumOperands() >= 3);

    PL_MEMCPY(
        reinterpret_cast<void *>(
            state.frame.GetValue(instruction->getOperand(0))),
        reinterpret_cast<void *>(
            state.frame.GetValue(instruction->getOperand(1))),
        static_cast<size_t>(state.frame.GetValue(instruction->getOperand(2))));
    return 0;
  } else if (intrinsic_name.substr(0, 12) == "llvm.memmove") {
    PL_ASSERT(instruction->getNumOperands() >= 3);

    std::memmove(
        reinterpret_cast<void *>(
            state.frame.GetValue(instruction->getOperand(0))),
        reinterpret_cast<void *>(
            state.frame.GetValue(instruction->getOperand(1))),
        static_cast<size_t>(state.frame.GetValue(instruction->getOperand(2))));
    return 0;
  } else if (intrinsic_name.substr(0, 11) == "llvm.memset") {
    PL_ASSERT(instruction->getNumOperands() >= 3);

    PL_MEMSET(
        reinterpret_cast<void *>(
            state.frame.GetValue(instruction->getOperand(0))),
        static_cast<uint8_t>(state.frame.GetValue(instruction->getOperand(1))),
        static_cast<size_t>(state.frame.GetValue(instruction->getOperand(2))));
    return 0;
  } else {
    throw Exception("unsupported llvm intrinsic");
  }
}

//----------------------------------------------------------------------------//
// Instruction execution functions
//----------------------------------------------------------------------------//

// Terminators

void QueryInterpreter::ExecuteBranch(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::BranchInst>(state.ip);

  if (instruction->isConditional()) {
    // check condition
    value_t condition = 1 & state.frame.GetValue(instruction->getOperand(0));

    // somehow the first operand is the false branch, while the second one
    // is the true one (llvm assembly is different!)

    if (condition > 0) {
      EnterBasicBlock(llvm::cast<llvm::BasicBlock>(instruction->getOperand(2)),
                      state);
    } else {
      EnterBasicBlock(llvm::cast<llvm::BasicBlock>(instruction->getOperand(1)),
                      state);
    }
  } else {
    EnterBasicBlock(llvm::cast<llvm::BasicBlock>(instruction->getOperand(0)),
                    state);
  }
}

void QueryInterpreter::ExecuteRet(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::ReturnInst>(state.ip);

  // if the return is not void, add the return value to the frame
  if (instruction->getNumOperands() > 0)
    state.return_value = state.frame.GetValue(instruction->getOperand(0));

  // set IP to nullptr to indicate that the function returns
  state.ip = nullptr;
}

// Standard binary operators

void QueryInterpreter::ExecuteBinaryIntegerOperator(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = state.ip;
  value_t result;

  // get operands
  size_t size = context_.GetTypeSize(instruction->getOperand(0)->getType());
  value_t op1 = state.frame.GetValue(instruction->getOperand(0));
  value_t op2 = state.frame.GetValue(instruction->getOperand(1));

  // because of two's complement, Add and Sub can be handled no matter
  // whether the value is signed or unsigned

  // perform actual operation
  switch (instruction->getOpcode()) {
    case llvm::Instruction::Add:
      result = op1 + op2;
      break;

    case llvm::Instruction::Sub:
      result = op1 - op2;
      break;

    case llvm::Instruction::Mul:
      result = op1 * op2;
      break;

    case llvm::Instruction::UDiv:
      result = op1 / op2;
      break;

    case llvm::Instruction::URem:
      result = op1 % op2;
      break;

    case llvm::Instruction::SDiv: {
      value_signed_t op1_signed =
          InterpreterUtils::ExtendSignedValue(op1, size);
      value_signed_t op2_signed =
          InterpreterUtils::ExtendSignedValue(op2, size);
      value_signed_t result_signed = op1_signed / op2_signed;
      result = InterpreterUtils::ShrinkSignedValue(result_signed, size);
      break;
    }

    case llvm::Instruction::SRem: {
      value_signed_t op1_signed =
          InterpreterUtils::ExtendSignedValue(op1, size);
      value_signed_t op2_signed =
          InterpreterUtils::ExtendSignedValue(op2, size);
      value_signed_t result_signed = op1_signed % op2_signed;
      result = InterpreterUtils::ShrinkSignedValue(result_signed, size);
      break;
    }

    default:
      throw Exception("unsupported type for binary integer operation");
  }

  // save value (masking is done inside SetValue()
  state.frame.SetValue(instruction, result);

  IncreaseIp(state);
}

void QueryInterpreter::DispatchBinaryFloatingPointOperator(
    FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::FPMathOperator>(state.ip);

  // get size of used type
  size_t size = context_.GetTypeSize(instruction->getOperand(0)->getType());

  // dispatch to the template implementation for this type size
  switch (size) {
    case sizeof(float):
      ExecuteBinaryFloatingPointOperator<float>(state);
      break;

    case sizeof(double):
      ExecuteBinaryFloatingPointOperator<double>(state);
      break;

    default:
      throw Exception("unsupported type for binary floating point operation");
  }
}

template <typename float_t>
void QueryInterpreter::ExecuteBinaryFloatingPointOperator(
    FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::FPMathOperator>(state.ip);
  value_t result;

  // get operands
  value_t op1 = state.frame.GetValue(instruction->getOperand(0));
  value_t op2 = state.frame.GetValue(instruction->getOperand(1));

  // make sure this is the correct function instance for the given type
  PL_ASSERT(sizeof(float_t) ==
            context_.GetTypeSize(instruction->getOperand(0)->getType()));

  float_t op1_float = *reinterpret_cast<float_t *>(&op1);
  float_t op2_float = *reinterpret_cast<float_t *>(&op2);
  float_t result_float;

  switch (instruction->getOpcode()) {
    case llvm::Instruction::FAdd:
      result_float = op1_float + op2_float;
      break;

    case llvm::Instruction::FSub:
      result_float = op1_float - op2_float;
      break;

    case llvm::Instruction::FMul:
      result_float = op1_float * op2_float;
      break;

    case llvm::Instruction::FDiv:
      result_float = op1_float / op2_float;
      break;

    case llvm::Instruction::FRem:
      result_float = std::fmod(op1_float, op2_float);
      break;

    default:
      throw Exception("unsupported floating point operation");
  }

  result = *reinterpret_cast<value_t *>(&result_float);

  // save value
  state.frame.SetValue(instruction, result);

  IncreaseIp(state);
}

// Logical operators

// Memory instructions

void QueryInterpreter::ExecuteLoad(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::LoadInst>(state.ip);

  // only simple loads are supported so far (non volatile, non atomic)
  PL_ASSERT(instruction->isSimple());

  // get the raw pointer to load from
  uintptr_t pointer = state.frame.GetValue(instruction->getPointerOperand());

  // get size of type that shall be loaded
  auto *pointer_type = llvm::cast<llvm::PointerType>(
      instruction->getPointerOperand()->getType());
  size_t type_size = context_.GetTypeSize(pointer_type->getElementType());

  // use MemCopy instead of simple pointer dereferencing to avoid a possible
  // segmentation fault if the value is smaller than 8 Bytes
  PL_ASSERT(type_size <= 8);
  value_t value;
  InterpreterUtils::MemCopy(&value, reinterpret_cast<void *>(pointer),
                            type_size);
  state.frame.SetValue(instruction, value);

  IncreaseIp(state);
}

void QueryInterpreter::ExecuteAlloca(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::AllocaInst>(state.ip);

  // get type
  llvm::Type *type = instruction->getAllocatedType();

  // get type size in bytes
  size_t type_size = context_.GetTypeSize(type);

  // calculate overall number of bytes needed
  size_t bytes_size;
  if (instruction->isArrayAllocation())
    bytes_size = type_size * state.frame.GetValue(instruction->getArraySize());
  else
    bytes_size = type_size;

  // get alignment
  unsigned int alignment = instruction->getAlignment();

  // allocate memory on stack frame
  uintptr_t pointer = state.frame.Alloca(bytes_size, alignment);

  // save pointer in this value
  state.frame.SetValue(instruction, pointer);

  IncreaseIp(state);
}

void QueryInterpreter::ExecuteGetElementPtr(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::GetElementPtrInst>(state.ip);

  // Get base pointer for calculation
  value_t pointer = state.frame.GetValue(instruction->getPointerOperand());

  // Get type of struct/array which will be processed
  llvm::Type *type = instruction->getSourceElementType();

  // First first_index in this instruction is the array first_index for the
  // source type
  value_signed_t first_index = InterpreterUtils::ExtendSignedValue(
      state.frame.GetValue(instruction->getOperand(1)),
      context_.GetTypeSize(instruction->getOperand(1)->getType()));
  long int delta = context_.GetTypeAllocSize(type) * first_index;

  // increase base pointer
  pointer += delta;

  LOG_TRACE("  index 1 (%ld): %s => + 0x%lX = 0x%016lX\n", first_index,
            InterpreterUtils::Print(instruction->getPointerOperand()->getType())
                .c_str(),
            delta, pointer);

  // Iterate remaining Indexes
  for (unsigned int operand_index = 2;
       operand_index < instruction->getNumOperands(); operand_index++) {
    auto *operand = instruction->getOperand(operand_index);
    size_t operand_size = context_.GetTypeSize(operand->getType());

    // first_index can be negative!
    value_signed_t index = InterpreterUtils::ExtendSignedValue(
        state.frame.GetValue(operand), operand_size);

    if (auto *array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
      // Advance pointer
      long int delta =
          context_.GetTypeAllocSize(array_type->getElementType()) * index;

      // increase base pointer
      pointer += delta;

      LOG_TRACE("  index %u (%ld): %s => + 0x%lX = 0x%016lX\n", operand_index,
                index, InterpreterUtils::Print(type).c_str(), delta, pointer);

      // get inner type for next iteration
      type = array_type->getElementType();
    } else if (auto *struct_type = llvm::dyn_cast<llvm::StructType>(type)) {
      PL_ASSERT(index < struct_type->getNumElements());
      long int delta = 0;

      for (unsigned int i = 0; i < index; i++) {
        llvm::Type *sub_type = struct_type->getElementType(i);

        // Add size of subtype to pointer
        delta += context_.GetTypeAllocSize(sub_type);
      }

      // increase base pointer
      pointer += delta;

      LOG_TRACE("  index %u (%ld): %s => + 0x%lX = 0x%016lX\n", operand_index,
                index, InterpreterUtils::Print(type).c_str(), delta, pointer);

      // get inner type for next iteration
      type = struct_type->getElementType(index);
    } else {
      throw Exception("unexpected type in getelementptr instruction: " +
                      InterpreterUtils::Print(type));
    }
  }

  // assure that resulting type is correct
  PL_ASSERT(type == instruction->getResultElementType());

  state.frame.SetValue(instruction, pointer);

  IncreaseIp(state);
}

// Convert instructions

void QueryInterpreter::ExecuteBitCast(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::BitCastInst>(state.ip);

  PL_ASSERT(instruction->isNoopCast(context_.GetDataLayout()));

  // Bitcast is a no-op, but we have to set a new value

  // make sure value fits in uint64_t
  PL_ASSERT(context_.GetTypeSize(instruction->getOperand(0)->getType()) <=
            sizeof(uint64_t));

  // get value and save it again associated with the new llvm::Value*
  uint64_t value = state.frame.GetValue(instruction->getOperand(0));
  state.frame.SetValue(instruction, value);

  IncreaseIp(state);
}

// Other instructions

void QueryInterpreter::ExecuteICmp(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::ICmpInst>(state.ip);

  // no vectors supported yet
  PL_ASSERT(!instruction->getOperand(1)->getType()->isVectorTy());

  // get operands
  value_t op1 = state.frame.GetValue(instruction->getOperand(0));
  value_t op2 = state.frame.GetValue(instruction->getOperand(1));
  size_t size = context_.GetTypeSize(instruction->getOperand(0)->getType());
  value_t result = 0;

  if (instruction->isUnsigned()) {
    // unsigned comparison: value_t is already unsigned int

    switch (instruction->getPredicate()) {
      case llvm::CmpInst::Predicate::ICMP_EQ:
        if (op1 == op2) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_NE:
        if (op1 != op2) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_UGT:
        if (op1 > op2) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_UGE:
        if (op1 >= op2) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_ULT:
        if (op1 < op2) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_ULE:
        if (op1 <= op2) result = 1;
        break;

      default: { throw Exception("unexpected comparison predicate"); }
    }
  } else {
    // signed comparison

    // extend value to make signed comparison
    value_signed_t op1_signed = InterpreterUtils::ExtendSignedValue(op1, size);
    value_signed_t op2_signed = InterpreterUtils::ExtendSignedValue(op2, size);

    switch (instruction->getPredicate()) {
      case llvm::CmpInst::Predicate::ICMP_EQ:
        if (op1_signed == op2_signed) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_NE:
        if (op1_signed != op2_signed) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_SGT:
        if (op1_signed > op2_signed) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_SGE:
        if (op1_signed >= op2_signed) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_SLT:
        if (op1_signed < op2_signed) result = 1;
        break;

      case llvm::CmpInst::Predicate::ICMP_SLE:
        if (op1_signed <= op2_signed) result = 1;
        break;

      default: { throw Exception("unexpected comparison predicate"); }
    }
  }

  // save result
  state.frame.SetValue(instruction, result);

  IncreaseIp(state);
}

void QueryInterpreter::ExecuteCall(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::CallInst>(state.ip);

  llvm::Function *function = instruction->getCalledFunction();
  value_t return_value;

  if (function->isDeclaration()) {
    // lookup function name
    std::string function_name = function->getName().str();

    if (function_name.substr(0, 5) == "llvm.") {
      // execute LLVM intrinsic
      return_value = ExecuteLLVMInstrinsic(instruction, function_name, state);
    } else {
      // function is not available in IR context, so we have to make an external
      // function call

      // lookup function in code context
      void *raw_pointer = context_.LookupBuiltinImpl(function_name);

      if (raw_pointer == nullptr) {
        throw Exception("could not find external function: " + function_name);
      }

      // prepare function arguments using ffi
      ffi_cif call_interface;
      size_t arguments_num = instruction->getNumArgOperands();
      std::vector<value_t> arguments(arguments_num);
      std::vector<void *> argument_pointers(arguments_num);
      std::vector<ffi_type *> argument_types(arguments_num);

      for (unsigned int i = 0; i < instruction->getNumArgOperands(); i++) {
        arguments[i] = state.frame.GetValue(instruction->getArgOperand(i));
        argument_pointers[i] = &arguments[i];

        argument_types[i] = InterpreterUtils::GetFFIType(
            context_, instruction->getArgOperand(i)->getType());
      }

      // define return type for ffi
      ffi_type *return_type =
          InterpreterUtils::GetFFIType(context_, instruction->getType());

      /* Initialize the cif */
      if (ffi_prep_cif(&call_interface, FFI_DEFAULT_ABI, arguments_num,
                       return_type, argument_types.data()) != FFI_OK) {
        throw Exception("initializing ffi call interface failed ");
      }

      // call external function
      ffi_call(&call_interface, reinterpret_cast<void (*)(void)>(raw_pointer),
               &return_value, argument_pointers.data());
    }
  } else {
    // internal function call to another IR function

    // prepare arguments
    std::vector<value_t> arguments(instruction->getNumArgOperands());
    for (unsigned int i = 0; i < instruction->getNumArgOperands(); i++) {
      arguments[i] = state.frame.GetValue(instruction->getArgOperand(i));
    }

    // call function
    return_value = CallFunction(function, arguments);
  }

  // set return value if provided
  if (!instruction->getType()->isVoidTy())
    state.frame.SetValue(instruction, return_value);

  IncreaseIp(state);
}

void QueryInterpreter::ExecuteSelect(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::SelectInst>(state.ip);

  // get condition
  value_t condition = 1 & state.frame.GetValue(instruction->getCondition());
  value_t result;

  if (condition > 0) {
    result = state.frame.GetValue(instruction->getTrueValue());
  } else {
    result = state.frame.GetValue(instruction->getFalseValue());
  }

  state.frame.SetValue(instruction, result);

  IncreaseIp(state);
}

void QueryInterpreter::ExecuteSExt(FunctionState &state) {
  // llvm::cast will throw an exception if the next instruction has the wrong
  // type
  auto *instruction = llvm::cast<llvm::SExtInst>(state.ip);

  // get value, current and destination type size
  value_t value = state.frame.GetValue(instruction->getOperand(0));
  value_t size_old = context_.GetTypeSize(instruction->getSrcTy());
  value_t size_new = context_.GetTypeSize(instruction->getDestTy());

  // do sign extension
  value_signed_t value_signed =
      InterpreterUtils::ExtendSignedValue(value, size_old, size_new);

  // save value
  state.frame.SetValue(instruction,
                       *reinterpret_cast<value_t *>(&value_signed));

  IncreaseIp(state);
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
