//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// context_builder.cpp
//
// Identification: src/codegen/interpreter/context_builder.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "codegen/interpreter/context_builder.h"
#include "codegen/codegen.h"
#include "codegen/code_context.h"

#include <limits>
#include <include/common/exception.h>
#include <llvm/IR/InstIterator.h>
#include <include/codegen/interpreter/interpreter_context.h>
#include <include/codegen/interpreter/context_builder.h>
#include <llvm/IR/CFG.h>

namespace peloton {
namespace codegen {
namespace interpreter {

ContextBuilder::ContextBuilder(CodeContext &code_context, llvm::Function *function) : number_values_(1), code_context_(code_context), llvm_function_(function)/*, bb_traversal_(function)*/ {}

InterpreterContext ContextBuilder::CreateInterpreterContext() {
  // DEBUG
#ifndef NDEBUG
  code_context_.DumpContents();
#endif

  InterpreterContext context;

  RegisterAllocation();
  TranslateFunction();

  context.number_values_ = number_values_;
  context.constants_ = std::move(constants_);
  context.bytecode_ = std::move(bytecode_);
  context.external_call_contexts_ = std::move(external_call_contexts_);
  context.number_arguments_ = number_arguments_;
  context.sub_contexts_ = sub_contexts_;

#ifndef NDEBUG
  context.instruction_trace_ = std::move(instruction_trace_);
#endif

  LOG_TRACE("%s", context.DumpContents().c_str());

  return std::move(context);
}

Opcode ContextBuilder::GetOpcodeForTypeAllTypes(Opcode untyped_op,
                                                llvm::Type *type) const {
  index_t id = InterpreterContext::GetOpcodeId(untyped_op);

  if (type == code_context_.bool_type_ || type == code_context_.int8_type_)
    return InterpreterContext::GetOpcodeFromId(id + 0);
  else if (type == code_context_.int16_type_)
    return InterpreterContext::GetOpcodeFromId(id + 1);
  else if (type == code_context_.int32_type_)
    return InterpreterContext::GetOpcodeFromId(id + 2);
  else if (type == code_context_.int64_type_ || type == code_context_.char_ptr_type_ || type->isPointerTy())
    return InterpreterContext::GetOpcodeFromId(id + 3);
  else if (type == code_context_.float_type_)
    return InterpreterContext::GetOpcodeFromId(id + 4);
  else if (type == code_context_.double_type_)
    return InterpreterContext::GetOpcodeFromId(id + 5);
  else
    throw NotSupportedException("llvm type not supported");
}

Opcode ContextBuilder::GetOpcodeForTypeIntTypes(Opcode untyped_op,
                                                llvm::Type *type) const {
  index_t id = InterpreterContext::GetOpcodeId(untyped_op);

  if (type == code_context_.bool_type_ || type == code_context_.int8_type_)
    return InterpreterContext::GetOpcodeFromId(id + 0);
  else if (type == code_context_.int16_type_)
    return InterpreterContext::GetOpcodeFromId(id + 1);
  else if (type == code_context_.int32_type_)
    return InterpreterContext::GetOpcodeFromId(id + 2);
  else if (type == code_context_.int64_type_ || type == code_context_.char_ptr_type_ || type->isPointerTy())
    return InterpreterContext::GetOpcodeFromId(id + 3);
  else
    throw NotSupportedException("llvm type not supported");
}

Opcode ContextBuilder::GetOpcodeForTypeFloatTypes(Opcode untyped_op,
                                                  llvm::Type *type) const {
  index_t id = InterpreterContext::GetOpcodeId(untyped_op);

  // float is missing!
  if (type == code_context_.float_type_)
    return InterpreterContext::GetOpcodeFromId(id + 0);
  if (type == code_context_.double_type_)
    return InterpreterContext::GetOpcodeFromId(id + 1);
  else
    throw NotSupportedException("llvm type not supported");
}

Opcode ContextBuilder::GetOpcodeForTypeSizeIntTypes(Opcode untyped_op,
                                                llvm::Type *type) const {
  index_t id = InterpreterContext::GetOpcodeId(untyped_op);

  switch (code_context_.GetTypeSize(type)) {
    case 1:
      return InterpreterContext::GetOpcodeFromId(id + 0);
    case 2:
      return InterpreterContext::GetOpcodeFromId(id + 1);
    case 4:
      return InterpreterContext::GetOpcodeFromId(id + 2);
    case 8:
      return InterpreterContext::GetOpcodeFromId(id + 3);
    default:
      throw NotSupportedException("llvm type size not supported");
  }
}

template <size_t number_instruction_slots>
Instruction &ContextBuilder::InsertBytecodeInstruction(UNUSED_ATTRIBUTE llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       index_t arg0,
                                                       index_t arg1,
                                                       index_t arg2,
                                                       index_t arg3,
                                                       index_t arg4,
                                                       index_t arg5,
                                                       index_t arg6) {
  PL_ASSERT(number_instruction_slots > 1 || (arg3 == 0 && arg4 == 0 && arg5 == 0 && arg6 == 0));

  instr_slot_t &slot = *bytecode_.insert(bytecode_.end(), number_instruction_slots, 0);
  Instruction& instruction = *reinterpret_cast<Instruction *>(&slot);
  instruction.op = opcode;
  instruction.args[0] = arg0;
  instruction.args[1] = arg1;
  instruction.args[2] = arg2;
  instruction.args[3] = arg3;
  instruction.args[4] = arg4;
  instruction.args[5] = arg5;
  instruction.args[6] = arg6;

#ifndef NDEBUG
  instruction_trace_.insert(instruction_trace_.end(), number_instruction_slots, llvm_instruction);
#endif

  return instruction;
}

Instruction &ContextBuilder::InsertBytecodeInstruction(llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       llvm::Value *arg0,
                                                       llvm::Value *arg1,
                                                       llvm::Value *arg2) {
  return InsertBytecodeInstruction(llvm_instruction, opcode, GetValueSlot(arg0), GetValueSlot(arg1), GetValueSlot(arg2));
}

Instruction &ContextBuilder::InsertBytecodeInstruction(llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       llvm::Value *arg0,
                                                       llvm::Value *arg1) {
  return InsertBytecodeInstruction(llvm_instruction, opcode, GetValueSlot(arg0), GetValueSlot(arg1));
}

ExternalCallInstruction &ContextBuilder::InsertBytecodeExternalCallInstruction(
    UNUSED_ATTRIBUTE llvm::Instruction *llvm_instruction,
    index_t call_context,
    void *function) {
  const size_t number_slots = std::ceil<size_t>(sizeof(ExternalCallInstruction) / sizeof(instr_slot_t));
  PL_ASSERT(number_slots == 2);

  instr_slot_t &slot = *bytecode_.insert(bytecode_.end(), number_slots, 0);

  ExternalCallInstruction instruction = {Opcode::call_external, call_context,
                                 reinterpret_cast<void (*)(void)>(function)};
  *reinterpret_cast<ExternalCallInstruction *>(&slot) = instruction;

#ifndef NDEBUG
  instruction_trace_.insert(instruction_trace_.end(), number_slots, llvm_instruction);
#endif

  return reinterpret_cast<ExternalCallInstruction &>(bytecode_[bytecode_.size() - number_slots]);
}

InternalCallInstruction &ContextBuilder::InsertBytecodeInternalCallInstruction(UNUSED_ATTRIBUTE llvm::Instruction *llvm_instruction,
                                                                   index_t interpreter_context,
                                                                   index_t dest_slot,
                                                                   size_t number_arguments) {
  const size_t number_slots = std::ceil<size_t>((3 + number_arguments) / sizeof(instr_slot_t));

  instr_slot_t &slot = *bytecode_.insert(bytecode_.end(), number_slots, 0);
  InternalCallInstruction& instruction = *reinterpret_cast<InternalCallInstruction *>(&slot);
  instruction.op = Opcode::call_internal;
  instruction.interpreter_context = interpreter_context;
  instruction.dest_slot = dest_slot;
  instruction.number_args = static_cast<index_t>(number_arguments);

#ifndef NDEBUG
  instruction_trace_.insert(instruction_trace_.end(), number_slots, llvm_instruction);
#endif

  return reinterpret_cast<InternalCallInstruction &>(bytecode_[bytecode_.size() - number_slots]);
}

bool ContextBuilder::IRLookAhead(llvm::Instruction *current_instruction, std::function<bool (llvm::Instruction *)> condition_check, llvm::Instruction *&next_instruction) {
  auto *next = current_instruction->getNextNode();
  if ((next != nullptr) && condition_check(next)) {
    next_instruction = next;
    return true;
  } else
    return false;
}


index_t ContextBuilder::AddConstant(llvm::Constant *constant) {
  llvm::Type *type = constant->getType();

  if (constant->isNullValue() || constant->isZeroValue()) {
    return AddConstant(static_cast<int64_t >(0));
  } else {
    switch (type->getTypeID()) {
      case llvm::Type::IntegerTyID: {
        int64_t value_signed =
            llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
        return AddConstant(value_signed);
      }

      case llvm::Type::FloatTyID: {
        float value_float = llvm::cast<llvm::ConstantFP>(constant)
            ->getValueAPF()
            .convertToFloat();
        value_t value = *reinterpret_cast<value_t *>(&value_float);
        const auto result = constants_.find(value);

        if (result != constants_.end()) {
          return result->second;
        } else {
          index_t slot = number_values_++;
          constants_[value] = slot;
          return slot;
        }
      }

      case llvm::Type::DoubleTyID: {
        double value_double = llvm::cast<llvm::ConstantFP>(constant)
            ->getValueAPF()
            .convertToDouble();;
        value_t value = *reinterpret_cast<value_t *>(&value_double);
        const auto result = constants_.find(value);

        if (result != constants_.end()) {
          return result->second;
        } else {
          index_t slot = number_values_++;
          constants_[value] = slot;
          return slot;
        }
      }

      case llvm::Type::PointerTyID: {
        if (constant->getNumOperands() > 0) {
          if (auto *constant_int =
              llvm::dyn_cast<llvm::ConstantInt>(constant->getOperand(0))) {
            value_t
                value = reinterpret_cast<value_t>(constant_int->getZExtValue());
            const auto result = constants_.find(value);

            if (result != constants_.end()) {
              return result->second;
            } else {
              index_t slot = number_values_++;
              constants_[value] = slot;
              return slot;
            }
          }
        }

        // fallthrough
      }

      default:
        throw NotSupportedException("unsupported constant type");
    }
  }
}

index_t ContextBuilder::AddConstant(int64_t value) {
  const auto result = constants_.find(value);

  if (result != constants_.end()) {
    return result->second;
  } else {
    index_t slot = number_values_++;
    constants_[value] = slot;
    return slot;
  }
}

index_t ContextBuilder::GetValueSlot(llvm::Value *value) {
  const auto result = value_mapping_.find(value);
  index_t slot;

  if (result != value_mapping_.end()) {
    return result->second;
  } else {
    if (auto *constant = llvm::dyn_cast<llvm::Constant>(value)) {
      slot = AddConstant(constant);
    } else {
      slot = number_values_++;
      value_mapping_[value] = slot;
    }

    return slot;
  }
}

void ContextBuilder::AddValueAlias(llvm::Value *value, llvm::Value *alias) {
  value_mapping_[alias] = value_mapping_[value];
}

index_t ContextBuilder::CreateAdditionalValueSlot() {
  index_t slot = number_values_++;
  return slot;
}

ffi_type *ContextBuilder::GetFFIType(llvm::Type *type) {
  if (type->isVoidTy()) {
    return &ffi_type_void;
  } else if (type->isPointerTy()) {
    return &ffi_type_pointer;
  } else if (type == code_context_.double_type_) {
    return &ffi_type_double;
  }

  // exact type not necessary, only size is important
  switch (code_context_.GetTypeSize(type)) {
    case 1:
      return &ffi_type_uint8;

    case 2:
      return &ffi_type_uint16;

    case 4:
      return &ffi_type_uint32;

    case 8:
      return &ffi_type_uint64;

    default:
      throw NotSupportedException("can't find a ffi_type for llvm::Type");
  }
}

bool ContextBuilder::IsConstantValue(llvm::Value *value) {
  auto *constant = llvm::dyn_cast<llvm::Constant>(value);
  return (constant != nullptr);
}

int64_t ContextBuilder::GetConstantIntegerValueSigned(llvm::Value *constant) {
  return llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
}

uint64_t ContextBuilder::GetConstantIntegerValueUnsigned(llvm::Value *constant) {
  return llvm::cast<llvm::ConstantInt>(constant)->getZExtValue();
}

void ContextBuilder::RegisterAllocation() {
  for (auto &argument : llvm_function_->args()) {
    index_t slot = number_values_++;
    value_mapping_[&argument] = slot;
  }

  /*

  typedef struct {
    size_t definition;
    size_t last_usage;
  } value_liveness_t;

  size_t index = 0;
  std::unordered_map<llvm::Value *, value_liveness_t> value_liveness;

  for (llvm::ReversePostOrderTraversal::rpo_iterator traversal_iterator = bb_traversal_.begin(); traversal_iterator != bb_traversal_.end(); ++traversal_iterator) {
    llvm::BasicBlock* bb = *traversal_iterator;

    for (llvm::BasicBlock::iterator instr_iterator = bb->begin(); instr_iterator != bb->end(); ++instr_iterator) {
      llvm::Instruction *instruction = instr_iterator;

      if (!instruction->getType()->isVoidTy()) {
        value_liveness[instruction] = {index, index};
      }

      for (llvm::Instruction::op_iterator op_iterator = instruction->op_begin(); op_iterator != instruction->op_end(); ++op_iterator) {
        llvm::Value *operand = op_iterator->get();
        auto liveness = value_liveness.find(operand);

        if (liveness != value_liveness.end()) {
          liveness->second.last_usage = index;
        } else {
          throw Exception("register allocation found usage before definition, violating SSA form");
        }
      }

      ++index;
    }
  }

  */
}

void ContextBuilder::TranslateFunction() {
  number_arguments_ = llvm_function_->getFunctionType()->getNumParams();

  for (llvm::Function::iterator bb_iterator = llvm_function_->begin(), bb_iterator_end = llvm_function_->end(); bb_iterator != bb_iterator_end; ++bb_iterator) {
    llvm::BasicBlock* bb = bb_iterator;

    bb_mapping_[bb] = bytecode_.size();

    for (llvm::BasicBlock::iterator instr_iterator = bb->begin(); instr_iterator != bb->end(); ++instr_iterator) {
      llvm::Instruction *instruction = instr_iterator;

      LOG_TRACE("Interpreter translating: %s\n", CodeGen::Print(instruction).c_str());

      switch (instruction->getOpcode()) {
        // Terminators
        case llvm::Instruction::Br:
          ProcessPHIsForBasicBlock(bb);
          TranslateBranch(instruction);
          break;

        case llvm::Instruction::Ret:
          ProcessPHIsForBasicBlock(bb);
          TranslateReturn(instruction);
          break;

          // Standard binary operators
          // Logical operators
        case llvm::Instruction::Add:
        case llvm::Instruction::Sub:
        case llvm::Instruction::Mul:
        case llvm::Instruction::UDiv:
        case llvm::Instruction::SDiv:
        case llvm::Instruction::URem:
        case llvm::Instruction::SRem:
        case llvm::Instruction::Shl:
        case llvm::Instruction::LShr:
        case llvm::Instruction::And:
        case llvm::Instruction::Or:
        case llvm::Instruction::Xor:
        case llvm::Instruction::AShr:
        case llvm::Instruction::FAdd:
        case llvm::Instruction::FSub:
        case llvm::Instruction::FMul:
        case llvm::Instruction::FDiv:
        case llvm::Instruction::FRem:
          TranslateBinaryOperator(instruction);
          break;

          // Memory instructions
        case llvm::Instruction::Load:
          TranslateLoad(instruction);
          break;

        case llvm::Instruction::Store:
          TranslateStore(instruction);
          break;

        case llvm::Instruction::Alloca:
          TranslateAlloca(instruction);
          break;

        case llvm::Instruction::GetElementPtr:
          TranslateGetElementPtr(instruction);
          break;

          // Cast instructions
        case llvm::Instruction::BitCast:
          // bit casts translate to nop
          //AddValueAlias(instruction->getOperand(0), instruction);
          InsertBytecodeInstruction(instruction, Opcode::nop_mov, instruction, instruction->getOperand(0));
          break;

        case llvm::Instruction::SExt:
        case llvm::Instruction::ZExt:
        case llvm::Instruction::IntToPtr:
          TranslateIntExt(instruction);
          break;

        case llvm::Instruction::Trunc:
        case llvm::Instruction::PtrToInt:
          // trunc translates to nop
          InsertBytecodeInstruction(instruction, Opcode::nop_mov, instruction, instruction->getOperand(0));
          //AddValueAlias(instruction->getOperand(0), instruction);
          break;

        case llvm::Instruction::UIToFP:
        case llvm::Instruction::SIToFP:
        case llvm::Instruction::FPToUI:
        case llvm::Instruction::FPToSI:
          TranslateFloatIntCast(instruction);

          // Other instructions
        case llvm::Instruction::ICmp:
        case llvm::Instruction::FCmp:
          TranslateCmp(instruction);
          break;

        case llvm::Instruction::PHI:
          // PHIs are handled before every terminating instruction
          break;

        case llvm::Instruction::Call:
          TranslateCall(instruction);
          break;

        case llvm::Instruction::Select:
          TranslateSelect(instruction);
          break;

        case llvm::Instruction::ExtractValue:
          TranslateExtractValue(instruction);
          break;

        case llvm::Instruction::Unreachable:
          // nop
          break;

          // Instruction is not supported
        default: {
          throw NotSupportedException("instruction not supported");
        }
      }
    }
  }

  for (auto &relocation : bytecode_relocations_) {
    reinterpret_cast<Instruction *>(&bytecode_[relocation.instruction_slot])->args[relocation.argument] = bb_mapping_[relocation.bb];
  }

  if (number_values_ >= std::numeric_limits<index_t>::max()) {
    throw NotSupportedException("number of values exceeds max number of bits");
  }
}

void ContextBuilder::ProcessPHIsForBasicBlock(llvm::BasicBlock *bb) {
  typedef struct {
    llvm::Instruction *instruction;
    index_t dest;
    index_t src;
  } additional_move_t;

  std::vector<additional_move_t> additional_moves;

  for (auto succ_iterator = llvm::succ_begin(bb); succ_iterator != llvm::succ_end(bb); ++succ_iterator) {
    for (auto instruction_iterator = succ_iterator->begin(); auto *phi_node = llvm::dyn_cast<llvm::PHINode>(&*instruction_iterator); ++instruction_iterator) {
      if (phi_node->getParent() == bb) {
        index_t temp_slot = CreateAdditionalValueSlot();

        InsertBytecodeInstruction(phi_node,
                                  Opcode::phi_mov,
                                  temp_slot,
                                  GetValueSlot(phi_node->getIncomingValueForBlock(
                                      bb)));
        additional_moves.push_back({phi_node, GetValueSlot(phi_node), temp_slot});
      } else {
        InsertBytecodeInstruction(phi_node,
                                  Opcode::phi_mov,
                                  GetValueSlot(phi_node),
                                  GetValueSlot(phi_node->getIncomingValueForBlock(
                                      bb)));
      }
    }
  }

  for (auto &entry : additional_moves) {
    InsertBytecodeInstruction(entry.instruction,
                              Opcode::phi_mov,
                              entry.dest,
                              entry.src);
  }
}

void ContextBuilder::TranslateBranch(llvm::Instruction *instruction) {
  auto *branch_instruction = llvm::cast<llvm::BranchInst>(&*instruction);

  if (branch_instruction->isConditional()) {
    // The first operand is the false branch, while the second one
    // is the true one (llvm assembly is the other way round)
    // To be consistent, we also use the order of the memory representation
    // in the bytecode.

    if (branch_instruction->getParent()->getNextNode() == branch_instruction->getOperand(1)) {
      InsertBytecodeInstruction(instruction,
                                Opcode::branch_cond_ft,
                                GetValueSlot(branch_instruction->getOperand(0)), 0);

      BytecodeRelocation relocation_false{static_cast<index_t>(bytecode_.size() - 1),
                                          1,
                                          llvm::cast<llvm::BasicBlock>(
                                              branch_instruction->getOperand(2))};
      bytecode_relocations_.push_back(relocation_false);
    } else {
      InsertBytecodeInstruction(instruction,
                                Opcode::branch_cond,
                                GetValueSlot(branch_instruction->getOperand(0)), 0, 0);

      BytecodeRelocation relocation_false{static_cast<index_t>(bytecode_.size() - 1),
                                          1,
                                          llvm::cast<llvm::BasicBlock>(
                                              branch_instruction->getOperand(1))};
      bytecode_relocations_.push_back(relocation_false);
      BytecodeRelocation relocation_true{static_cast<index_t>(bytecode_.size() - 1),
                                         2,
                                         llvm::cast<llvm::BasicBlock>(
                                             branch_instruction->getOperand(2))};
      bytecode_relocations_.push_back(relocation_true);
    }
  } else {
    if (branch_instruction->getParent()->getNextNode() != branch_instruction->getOperand(0)) {
      InsertBytecodeInstruction(instruction, Opcode::branch_uncond, 0);

      BytecodeRelocation relocation{static_cast<index_t>(bytecode_.size() - 1),
                                    0,
                                    llvm::cast<llvm::BasicBlock>(
                                        branch_instruction->getOperand(0))};
      bytecode_relocations_.push_back(relocation);
    }
  }
}

void ContextBuilder::TranslateReturn(llvm::Instruction *instruction) {
  auto *return_instruction = llvm::cast<llvm::ReturnInst>(&*instruction);

  index_t return_slot = 0;
  if (return_instruction->getNumOperands() > 0)
    return_slot = GetValueSlot(return_instruction->getOperand(0));

  InsertBytecodeInstruction(instruction, Opcode::ret, return_slot);
}

void ContextBuilder::TranslateBinaryOperator(llvm::Instruction *instruction) {
  auto *binary_operator = llvm::cast<llvm::BinaryOperator>(&*instruction);
  auto *type = binary_operator->getType();
  Opcode opcode;

  switch (binary_operator->getOpcode()) {
    case llvm::Instruction::Add:
    case llvm::Instruction::FAdd:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::add), type);
      break;

    case llvm::Instruction::Sub:
    case llvm::Instruction::FSub:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::sub), type);
      break;

    case llvm::Instruction::Mul:
    case llvm::Instruction::FMul:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::mul), type);
      break;

    case llvm::Instruction::UDiv:
    case llvm::Instruction::FDiv:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::div), type);
      break;

    case llvm::Instruction::SDiv:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sdiv), type);
      break;

    case llvm::Instruction::URem:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::urem), type);
      break;

    case llvm::Instruction::FRem:
      opcode = GetOpcodeForTypeFloatTypes(GET_FIRST_FLOAT_TYPES(Opcode::frem), type);
      break;

    case llvm::Instruction::SRem:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::srem), type);
      break;

    case llvm::Instruction::Shl:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::shl), type);
      break;

    case llvm::Instruction::LShr:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::lshr), type);
      break;

    case llvm::Instruction::AShr:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::ashr), type);
      break;

    case llvm::Instruction::And:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::and), type);
      break;

    case llvm::Instruction::Or:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::or), type);
      break;

    case llvm::Instruction::Xor:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::xor), type);
      break;

    default:
      throw NotSupportedException("binary operation not supported");
  }

  InsertBytecodeInstruction(instruction,
                            opcode,
                            binary_operator,
                            binary_operator->getOperand(0),
                            binary_operator->getOperand(1));
}

void ContextBuilder::TranslateAlloca(llvm::Instruction *instruction) {
  auto *alloca_instruction = llvm::cast<llvm::AllocaInst>(&*instruction);
  Opcode opcode;

  // get type to allocate
  llvm::Type *type = alloca_instruction->getAllocatedType();

  // get type size in bytes
  size_t type_size = code_context_.GetTypeSize(type);

  index_t array_size;
  if (alloca_instruction->isArrayAllocation()) {
    array_size = GetValueSlot(alloca_instruction->getArraySize());
    opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::alloca), alloca_instruction->getArraySize()->getType());
  } else {
    array_size = AddConstant(1);
    opcode = Opcode::alloca_i64;
  }

  // type size is immediate value!
  InsertBytecodeInstruction(instruction, opcode,
                            GetValueSlot(alloca_instruction),
                            static_cast<index_t>(type_size),
                            array_size);
}

void ContextBuilder::TranslateLoad(llvm::Instruction *instruction) {
  auto *load_instruction = llvm::cast<llvm::LoadInst>(&*instruction);

  Opcode opcode = GetOpcodeForTypeSizeIntTypes(GET_FIRST_INT_TYPES(Opcode::load), load_instruction->getType());
  InsertBytecodeInstruction(instruction,
                            opcode,
                            load_instruction,
                            load_instruction->getPointerOperand());
}

void ContextBuilder::TranslateStore(llvm::Instruction *instruction) {
  auto *store_instruction = llvm::cast<llvm::StoreInst>(&*instruction);

  Opcode opcode = GetOpcodeForTypeSizeIntTypes(GET_FIRST_INT_TYPES(Opcode::store), store_instruction->getOperand(0)->getType());
  InsertBytecodeInstruction(instruction,
                            opcode,
                            store_instruction->getPointerOperand(),
                            store_instruction->getValueOperand());
}

void ContextBuilder::TranslateGetElementPtr(llvm::Instruction *instruction) {
  auto *gep_instruction = llvm::cast<llvm::GetElementPtrInst>(&*instruction);
  int64_t overall_offset = 0;

  // offset is an immediate constant, not a slot index
  // instruction is created here, but offset will be filled in later
  auto &gep_offset_bytecode_instruction = InsertBytecodeInstruction(
      gep_instruction,
      Opcode::gep_offset,
      GetValueSlot(gep_instruction),
      GetValueSlot(gep_instruction->getPointerOperand()),
      0);

  // First index operand of the instruction is the array index for the
  // source type

  // Get type of struct/array which will be processed
  llvm::Type *type = gep_instruction->getSourceElementType();

  if (IsConstantValue(gep_instruction->getOperand(1))) {
    overall_offset += code_context_.GetTypeSize(type) * GetConstantIntegerValueSigned(gep_instruction->getOperand(1));
  } else {
    index_t index = GetValueSlot(instruction->getOperand(1));
    Opcode opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::gep_array), instruction->getOperand(1)->getType());

    // size of array element is an immediate constant, not a slot index!
    InsertBytecodeInstruction(gep_instruction,
                              opcode,
                              GetValueSlot(gep_instruction),
                              index,
                              static_cast<index_t>(code_context_.GetTypeSize(
                                  type)));
  }

  // Iterate remaining Indexes
  for (unsigned int operand_index = 2;
       operand_index < instruction->getNumOperands(); ++operand_index) {
    auto *operand = instruction->getOperand(operand_index);

    if (auto *array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
      if (IsConstantValue(operand)) {
        overall_offset += code_context_.GetTypeSize(array_type->getElementType()) * GetConstantIntegerValueSigned(operand);
      } else {
        index_t index = GetValueSlot(operand);
        Opcode opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::gep_array),
                                                 operand->getType());

        // size of array element is an immediate constant, not a slot index!
        InsertBytecodeInstruction(gep_instruction,
                                  opcode,
                                  GetValueSlot(gep_instruction),
                                  index,
                                  static_cast<index_t>(code_context_.GetTypeSize(
                                      array_type->getElementType())));
      }

      // get inner type for next iteration
      type = array_type->getElementType();

    } else if (auto *struct_type = llvm::dyn_cast<llvm::StructType>(type)) {
      uint64_t index = GetConstantIntegerValueUnsigned(operand);
      PL_ASSERT(index < struct_type->getNumElements());
      size_t offset = 0;

      for (unsigned int i = 0; i < index; i++) {
        llvm::Type *sub_type = struct_type->getElementType(i);

        // TODO(marcel): double check different GetSize methods

        // Add size of subtype to pointer
        offset += code_context_.GetTypeAllocSize(sub_type);
      }

      overall_offset += offset;

      // get inner type for next iteration
      type = struct_type->getElementType(index);

    } else {
      throw NotSupportedException("unexpected type in getelementptr instruction");
    }
  }

  // TODO(marcel): throw exception when value too big for 2 byte

  // assure that resulting type is correct
  PL_ASSERT(type == gep_instruction->getResultElementType());

  // TODO: check if gep can be left out
  if ((reinterpret_cast<instr_slot_t *>(&gep_offset_bytecode_instruction) == &bytecode_.back()) && overall_offset == 0) {
    // TODO: create alias!
    //printf("found zero gep: %s\n", CodeGen::Print(instruction).c_str());
  }

  // fill in calculated overall offset in previously placed gep_offset
  // bytecode instruction
  gep_offset_bytecode_instruction.args[2] = static_cast<index_t>(overall_offset);

}

void ContextBuilder::TranslateFloatIntCast(llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  Opcode opcode = Opcode::undefined;

  if (instruction->getOpcode() == llvm::Instruction::FPToSI) {
    if (cast_instruction->getOperand(0)->getType() != code_context_.float_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::floattosi);
    else if (cast_instruction->getOperand(0)->getType() != code_context_.double_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::doubletosi);
    else
      throw NotSupportedException("llvm fp type not supported");

    opcode = GetOpcodeForTypeIntTypes(opcode, cast_instruction->getType());

  } else if (instruction->getOpcode() == llvm::Instruction::FPToUI) {
    if (cast_instruction->getOperand(0)->getType() != code_context_.float_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::floattoui);
    else if (cast_instruction->getOperand(0)->getType() != code_context_.double_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::doubletoui);
    else
      throw NotSupportedException("llvm fp type not supported");

    opcode = GetOpcodeForTypeIntTypes(opcode, cast_instruction->getType());

  } else if (instruction->getOpcode() == llvm::Instruction::SIToFP) {
    if (cast_instruction->getType() != code_context_.float_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::sitofloat);
    else if (cast_instruction->getType() != code_context_.double_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::sitodouble);
    else
      throw NotSupportedException("llvm fp type not supported");

    opcode = GetOpcodeForTypeIntTypes(opcode, cast_instruction->getOperand(0)->getType());

  } else if (instruction->getOpcode() == llvm::Instruction::UIToFP) {
    if (cast_instruction->getType() != code_context_.float_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::uitofloat);
    else if (cast_instruction->getType() != code_context_.double_type_)
      opcode = GET_FIRST_INT_TYPES(Opcode::uitodouble);
    else
      throw NotSupportedException("llvm fp type not supported");

    opcode = GetOpcodeForTypeIntTypes(opcode, cast_instruction->getOperand(0)->getType());

  } else {
    throw NotSupportedException("unsupported cast instruction");
  }

  InsertBytecodeInstruction(cast_instruction, opcode, cast_instruction, cast_instruction->getOperand(0));
}

void ContextBuilder::TranslateIntExt(llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  size_t src_type_size = code_context_.GetTypeSize(cast_instruction->getSrcTy());
  size_t dest_type_size = code_context_.GetTypeSize(cast_instruction->getDestTy());

  if (src_type_size == dest_type_size) {
    InsertBytecodeInstruction(instruction, Opcode::nop_mov, instruction, instruction->getOperand(0));
    return;
  }

  Opcode opcode = Opcode::undefined;

  if (instruction->getOpcode() == llvm::Instruction::SExt) {
    if (src_type_size == 1 && dest_type_size == 2) {
      opcode = Opcode::sext_i8_i16;
    } else if (src_type_size == 1 && dest_type_size == 4) {
      opcode = Opcode::sext_i8_i32;
    } else if (src_type_size == 1 && dest_type_size == 8) {
      opcode = Opcode::sext_i8_i64;
    } else if (src_type_size == 2 && dest_type_size == 4) {
      opcode = Opcode::sext_i16_i32;
    } else if (src_type_size == 2 && dest_type_size == 8) {
      opcode = Opcode::sext_i16_i64;
    } else if (src_type_size == 4 && dest_type_size == 8) {
      opcode = Opcode::sext_i32_i64;
    } else {
      throw NotSupportedException("unsupported sext instruction");
    }

  } else if (instruction->getOpcode() == llvm::Instruction::ZExt ||
      instruction->getOpcode() == llvm::Instruction::IntToPtr) {
    if (src_type_size == 1 && dest_type_size == 2) {
      opcode = Opcode::zext_i8_i16;
    } else if (src_type_size == 1 && dest_type_size == 4) {
      opcode = Opcode::zext_i8_i32;
    } else if (src_type_size == 1 && dest_type_size == 8) {
      opcode = Opcode::zext_i8_i64;
    } else if (src_type_size == 2 && dest_type_size == 4) {
      opcode = Opcode::zext_i16_i32;
    } else if (src_type_size == 2 && dest_type_size == 8) {
      opcode = Opcode::zext_i16_i64;
    } else if (src_type_size == 4 && dest_type_size == 8) {
      opcode = Opcode::zext_i32_i64;
    } else {
      throw NotSupportedException("unsupported zext instruction");
    }

  } else {
    throw NotSupportedException("unexpected ext instruction");
  }

  InsertBytecodeInstruction(cast_instruction, opcode, cast_instruction, cast_instruction->getOperand(0));
}

void ContextBuilder::TranslateCmp(llvm::Instruction *instruction) {
  auto *cmp_instruction = llvm::cast<llvm::CmpInst>(&*instruction);
  auto *type = cmp_instruction->getOperand(0)->getType();
  Opcode opcode = Opcode::undefined;

  switch (cmp_instruction->getPredicate()) {
    case llvm::CmpInst::Predicate::ICMP_EQ:
    case llvm::CmpInst::Predicate::FCMP_OEQ:
    case llvm::CmpInst::Predicate::FCMP_UEQ:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_eq), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_NE:
    case llvm::CmpInst::Predicate::FCMP_ONE:
    case llvm::CmpInst::Predicate::FCMP_UNE:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_ne), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGT:
    case llvm::CmpInst::Predicate::FCMP_OGT:
    case llvm::CmpInst::Predicate::FCMP_UGT:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_gt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGE:
    case llvm::CmpInst::Predicate::FCMP_OGE:
    case llvm::CmpInst::Predicate::FCMP_UGE:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_ge), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULT:
    case llvm::CmpInst::Predicate::FCMP_OLT:
    case llvm::CmpInst::Predicate::FCMP_ULT:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_lt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULE:
    case llvm::CmpInst::Predicate::FCMP_OLE:
    case llvm::CmpInst::Predicate::FCMP_ULE:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_le), type);
      break;


    case llvm::CmpInst::Predicate::ICMP_SGT:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sgt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SGE:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sge), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SLT:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_slt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SLE:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sle), type);
      break;

    default:
      throw NotSupportedException("compare operand not supported");
  }

  InsertBytecodeInstruction(cmp_instruction,
                            opcode,
                            cmp_instruction,
                            cmp_instruction->getOperand(0),
                            cmp_instruction->getOperand(1));
}

void ContextBuilder::TranslateCall(llvm::Instruction *instruction) {
  auto *call_instruction = llvm::cast<llvm::CallInst>(&*instruction);

  llvm::Function *function = call_instruction->getCalledFunction();

  if (function->isDeclaration()) {
    // lookup function name
    std::string function_name = function->getName().str();

    if (function_name.substr(0, 11) == "llvm.memcpy") {
      if (call_instruction->getOperand(2)->getType() != code_context_.int64_type_)
        throw NotSupportedException("memcpy with different size type than i64 not supported");

      InsertBytecodeInstruction(call_instruction, Opcode::llvm_memcpy,
                                call_instruction->getOperand(0),
                                call_instruction->getOperand(1),
                                call_instruction->getOperand(2));

    } else if (function_name.substr(0, 12) == "llvm.memmove") {
      if (call_instruction->getOperand(2)->getType() != code_context_.int64_type_)
        throw NotSupportedException("memmove with different size type than i64 not supported");

      InsertBytecodeInstruction(call_instruction, Opcode::llvm_memmove,
                                call_instruction->getOperand(0),
                                call_instruction->getOperand(1),
                                call_instruction->getOperand(2));

    } else if (function_name.substr(0, 11) == "llvm.memset") {
      if (call_instruction->getOperand(2)->getType() != code_context_.int64_type_)
        throw NotSupportedException("memset with different size type than i64 not supported");

      InsertBytecodeInstruction(call_instruction, Opcode::llvm_memset,
                                call_instruction->getOperand(0),
                                call_instruction->getOperand(1),
                                call_instruction->getOperand(2));

    } else if (function_name.substr(10, 13) == "with.overflow") {
      index_t result = 0;
      index_t overflow = 0;
      auto *type = call_instruction->getOperand(0)->getType();
      Opcode opcode = Opcode::undefined;

      if (call_instruction->getNumUses() > 2)
        throw NotSupportedException("*.with.overflow intrinsics with more than two uses not supported");

      for(auto *user : call_instruction->users()) {
        auto *extract_instruction = llvm::cast<llvm::ExtractValueInst>(user);
        size_t extract_index = *extract_instruction->idx_begin();

        PL_ASSERT(extract_index == 0|| extract_index == 1);

        if (extract_index == 0) {
          result = GetValueSlot(extract_instruction);

        } else if (extract_index == 1) {
          overflow = GetValueSlot(extract_instruction);
        }
      }

      if (function_name.substr(5, 4) == "uadd") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_uadd_overflow), type);
      } else if (function_name.substr(5, 4) == "sadd") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_sadd_overflow), type);
      } else if (function_name.substr(5, 4) == "usub") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_usub_overflow), type);
      } else if (function_name.substr(5, 4) == "ssub") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_ssub_overflow), type);
      } else if (function_name.substr(5, 4) == "umul") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_umul_overflow), type);
      } else if (function_name.substr(5, 4) == "smul") {
        opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::llvm_smul_overflow), type);
      } else {
        throw NotSupportedException("the requested operation with overflow is not supported");
      }

      InsertBytecodeInstruction<2>(call_instruction,
                                   opcode,
                                   result,
                                   overflow,
                                   GetValueSlot(call_instruction->getOperand(0)),
                                   GetValueSlot(call_instruction->getOperand(1)));
    } else if (function_name.substr(0, 26) == "llvm.x86.sse42.crc32.64.64") {
      if (call_instruction->getType() != code_context_.int64_type_)
        throw NotSupportedException("sse42.crc32 with different size type than i64 not supported");

      InsertBytecodeInstruction(call_instruction, Opcode::llvm_sse42_crc32,
                                call_instruction,
                                call_instruction->getOperand(0),
                                call_instruction->getOperand(1));
    } else {
      // function is not available in IR context, so we have to make an external
      // function call

      // lookup function in code context
      void *raw_pointer = code_context_.LookupBuiltinImpl(function_name);

      if (raw_pointer == nullptr) {
        throw NotSupportedException("could not find external function: " + function_name);
      }

      index_t dest_slot = 0;
      if (!instruction->getType()->isVoidTy())
        dest_slot = GetValueSlot(call_instruction);

      size_t arguments_num = call_instruction->getNumArgOperands();
      ExternalCallContext call_context{dest_slot,
                               GetFFIType(instruction->getType()),
                               std::vector<index_t>(arguments_num),
                               std::vector<ffi_type *>(arguments_num)};

      for (unsigned int i = 0; i < call_instruction->getNumArgOperands(); i++) {
        call_context.args[i] = GetValueSlot(call_instruction->getArgOperand(i));
        call_context.arg_types[i] = GetFFIType(call_instruction->getArgOperand(i)->getType());
      }

      // init later because of absolute pointers

      external_call_contexts_.push_back(call_context);

      InsertBytecodeExternalCallInstruction(call_instruction,
                                            static_cast<index_t>(
                                                external_call_contexts_.size() - 1),
                                            raw_pointer);
    }
  } else {
    // internal function call to another IR function

    index_t dest_slot = 0;
    if (!instruction->getType()->isVoidTy())
      dest_slot = GetValueSlot(call_instruction);

    index_t interpreter_context_index;
    const auto result = sub_context_mapping_.find(function);
    if (result != sub_context_mapping_.end()) {
      interpreter_context_index = result->second;
    } else {
      ContextBuilder sub_context_builder(code_context_, function);
      auto sub_context = sub_context_builder.CreateInterpreterContext();

      sub_contexts_.push_back(std::move(sub_context));
      interpreter_context_index = sub_contexts_.size() - 1;
      sub_context_mapping_[function] = interpreter_context_index;
    }

    InternalCallInstruction &instruction = InsertBytecodeInternalCallInstruction(call_instruction, interpreter_context_index, dest_slot, call_instruction->getNumArgOperands());

    for (unsigned int i = 0; i < call_instruction->getNumArgOperands(); i++) {
      instruction.args[i] = GetValueSlot(call_instruction->getArgOperand(i));

      if (code_context_.GetTypeSize(call_instruction->getArgOperand(i)->getType()) > 8) {
        throw NotSupportedException("argument for internal call too big");
      }
    }
  }
}

void ContextBuilder::TranslateSelect(llvm::Instruction *instruction) {
  auto *select_instruction = llvm::cast<llvm::SelectInst>(&*instruction);

  InsertBytecodeInstruction<2>(select_instruction,
                               Opcode::select,
                               GetValueSlot(select_instruction),
                               GetValueSlot(select_instruction->getCondition()),
                               GetValueSlot(select_instruction->getTrueValue()),
                               GetValueSlot(select_instruction->getFalseValue()));
}

void ContextBuilder::TranslateExtractValue(llvm::Instruction *instruction) {
  auto *extract_instruction = llvm::cast<llvm::ExtractValueInst>(&*instruction);

  // Skip this instruction if it belongs to an overflow operation, as it
  // has been handled there already
  if (llvm::CallInst *call_instruction = llvm::dyn_cast<llvm::CallInst>(extract_instruction->getOperand(0))) {
    llvm::Function *function = call_instruction->getCalledFunction();
    if (function->isDeclaration()) {
      // lookup function name
      std::string function_name = function->getName().str();
      if (function_name.substr(10, 13) == "with.overflow") {
        return;
      }
    }
  }

  // Get value type
  llvm::Type *type = extract_instruction->getAggregateOperand()->getType();
  size_t offset_bits = 0;

  // make sure the result type fits in a value_t
  if (code_context_.GetTypeSize(instruction->getType()) <= sizeof(value_t)) {
    throw NotSupportedException("extracted value too big for register size");
  }

  // Iterate indexes
  for (auto index_it = extract_instruction->idx_begin(),
           index_end = extract_instruction->idx_end();
       index_it != index_end; index_it++) {
    uint32_t index = *index_it;

    if (auto *array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
      // Advance offset
      offset_bits +=
          code_context_.GetTypeAllocSizeInBits(array_type->getElementType()) * index;

      // get inner type for next iteration
      type = array_type->getElementType();
    } else if (auto *struct_type = llvm::dyn_cast<llvm::StructType>(type)) {
      PL_ASSERT(index < struct_type->getNumElements());

      for (unsigned int i = 0; i < index; i++) {
        llvm::Type *sub_type = struct_type->getElementType(i);

        // Add size of subtype to offset
        offset_bits += code_context_.GetTypeAllocSizeInBits(sub_type);
      }

      // get inner type for next iteration
      type = struct_type->getElementType(index);
    } else {
      throw NotSupportedException("unexpected type in extractvalue instruction");
    }
  }

  // assure that resulting type is correct
  PL_ASSERT(type == extract_instruction->getType());

  // number if bits to shift is an immediate value!
  InsertBytecodeInstruction(extract_instruction, Opcode::extractvalue, GetValueSlot(extract_instruction), GetValueSlot(extract_instruction->getAggregateOperand()),
                            static_cast<index_t>(offset_bits));
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
