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
#include "include/common/exception.h"

#include <llvm/IR/InstIterator.h>

namespace peloton {
namespace codegen {
namespace interpreter {

ContextBuilder::ContextBuilder(const CodeContext &code_context, const llvm::Function *function) : number_value_slots_(0), number_temporary_value_slots_(0), rpo_traversal_(function), code_context_(code_context), llvm_function_(function) {}

InterpreterContext ContextBuilder::CreateInterpreterContext(const CodeContext &code_context, const llvm::Function *function) {
  ContextBuilder builder(code_context, function);

  builder.AnalyseFunction();
  builder.PerformNaiveRegisterAllocation();
  //builder.PerformGreedyRegisterAllocation();

#ifndef NDEBUG
  // DEBUG
  code_context.DumpContents();

  printf("IR:\n");
  for (unsigned i = 0; i < builder.bb_reverse_post_order_.size(); i++) {
    printf("%u:%s", i, CodeGen::Print(builder.bb_reverse_post_order_[i]).c_str());
  }

  printf("Mapping:\n");
  for (unsigned int i = 0; i < builder.value_slots_.size(); i++) {
    if (builder.value_liveness_[i].last_usage < valueLivenessUnknown) {
      printf("%u;%u;%u\n",
               builder.value_slots_[i],
               builder.value_liveness_[i].definition,
               builder.value_liveness_[i].last_usage);

      /*
      for (auto &mapping : builder.value_mapping_) {
        if (mapping.second == i) {
          printf("%s\n", CodeGen::Print(mapping.first).c_str());
        }
      }
       */
    }
  }
  printf("--\n");
#endif

  builder.TranslateFunction();
  builder.Finalize();

  LOG_DEBUG("%s", builder.context_.DumpContents().c_str());

  return std::move(builder.context_);
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
    throw NotSupportedException("llvm type not supported: " + CodeGen::Print(type));
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
    throw NotSupportedException("llvm type not supported: " + CodeGen::Print(type));
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
    throw NotSupportedException("llvm type not supported: " + CodeGen::Print(type));
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
      throw NotSupportedException("llvm type size not supported: " + CodeGen::Print(type));
  }
}

template <size_t number_instruction_slots>
Instruction &ContextBuilder::InsertBytecodeInstruction(UNUSED_ATTRIBUTE const llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       index_t arg0,
                                                       index_t arg1,
                                                       index_t arg2,
                                                       index_t arg3,
                                                       index_t arg4,
                                                       index_t arg5,
                                                       index_t arg6) {
  PL_ASSERT(opcode != Opcode::undefined);
  PL_ASSERT(number_instruction_slots > 1 || (arg3 == 0 && arg4 == 0 && arg5 == 0 && arg6 == 0));

  instr_slot_t &slot = *context_.bytecode_.insert(context_.bytecode_.end(), number_instruction_slots, 0);
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
  context_.instruction_trace_.insert(context_.instruction_trace_.end(), number_instruction_slots, llvm_instruction);
#endif

  return instruction;
}

Instruction &ContextBuilder::InsertBytecodeInstruction(const llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       const llvm::Value *arg0,
                                                       const llvm::Value *arg1,
                                                       const llvm::Value *arg2) {
  PL_ASSERT(opcode != Opcode::undefined);
  return InsertBytecodeInstruction(llvm_instruction, opcode, GetValueSlot(arg0), GetValueSlot(arg1), GetValueSlot(arg2));
}

Instruction &ContextBuilder::InsertBytecodeInstruction(const llvm::Instruction *llvm_instruction,
                                                       Opcode opcode,
                                                       const llvm::Value *arg0,
                                                       const llvm::Value *arg1) {
  PL_ASSERT(opcode != Opcode::undefined);
  return InsertBytecodeInstruction(llvm_instruction, opcode, GetValueSlot(arg0), GetValueSlot(arg1));
}

ExternalCallInstruction &ContextBuilder::InsertBytecodeExternalCallInstruction(
    UNUSED_ATTRIBUTE const llvm::Instruction *llvm_instruction,
    index_t call_context,
    void *function) {
  const size_t number_slots = (sizeof(ExternalCallInstruction) + sizeof(instr_slot_t) - 1) / sizeof(instr_slot_t);
  PL_ASSERT(number_slots == 2);

  instr_slot_t &slot = *context_.bytecode_.insert(context_.bytecode_.end(), number_slots, 0);

  ExternalCallInstruction instruction = {Opcode::call_external, call_context,
                                 reinterpret_cast<void (*)(void)>(function)};
  *reinterpret_cast<ExternalCallInstruction *>(&slot) = instruction;

#ifndef NDEBUG
  context_.instruction_trace_.insert(context_.instruction_trace_.end(), number_slots, llvm_instruction);
#endif

  return reinterpret_cast<ExternalCallInstruction &>(context_.bytecode_[context_.bytecode_.size() - number_slots]);
}

InternalCallInstruction &ContextBuilder::InsertBytecodeInternalCallInstruction(UNUSED_ATTRIBUTE const llvm::Instruction *llvm_instruction,
                                                                   index_t sub_context,
                                                                   index_t dest_slot,
                                                                   size_t number_arguments) {
  const size_t number_slots = ((2 * (4 + number_arguments)) + sizeof(instr_slot_t) - 1) / sizeof(instr_slot_t);

  instr_slot_t &slot = *context_.bytecode_.insert(context_.bytecode_.end(), number_slots, 0);
  InternalCallInstruction& instruction = *reinterpret_cast<InternalCallInstruction *>(&slot);
  instruction.op = Opcode::call_internal;
  instruction.sub_context = sub_context;
  instruction.dest_slot = dest_slot;
  instruction.number_args = static_cast<index_t>(number_arguments);

  PL_ASSERT(&instruction.args[number_arguments - 1] < reinterpret_cast<index_t *>(&context_.bytecode_.back() + 1));

#ifndef NDEBUG
  context_.instruction_trace_.insert(context_.instruction_trace_.end(), number_slots, llvm_instruction);
#endif

  return reinterpret_cast<InternalCallInstruction &>(slot);
}

ContextBuilder::value_index_t ContextBuilder::CreateValueAlias(const llvm::Value *alias, value_index_t value_index) {
  PL_ASSERT(value_mapping_.find(alias) == value_mapping_.end());
  value_mapping_[alias] = value_index;

  return value_index;
}

ContextBuilder::value_index_t ContextBuilder::CreateValueIndex(const llvm::Value *value) {
  PL_ASSERT(value_mapping_.find(value) == value_mapping_.end());

  value_index_t value_index = value_liveness_.size();
  value_mapping_[value] = value_index;
  value_liveness_.push_back({valueLivenessUnknown, valueLivenessUnknown});

  return value_index;
}

ContextBuilder::value_index_t ContextBuilder::GetValueIndex(const llvm::Value *value) const {
  PL_ASSERT(value_mapping_.find(value) != value_mapping_.end());
  return value_mapping_.at(value);
}


value_t ContextBuilder::GetConstantValue(const llvm::Constant *constant) const {
  llvm::Type *type = constant->getType();

  if (constant->isNullValue() || constant->isZeroValue()) {
    return 0;
  } else {
    switch (type->getTypeID()) {
      case llvm::Type::IntegerTyID: {
        int64_t value_signed = llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
        return *reinterpret_cast<value_t *>(&value_signed);
      }

      case llvm::Type::FloatTyID: {
        float value_float = llvm::cast<llvm::ConstantFP>(constant)
            ->getValueAPF()
            .convertToFloat();
        return *reinterpret_cast<value_t *>(&value_float);
      }

      case llvm::Type::DoubleTyID: {
        double value_double = llvm::cast<llvm::ConstantFP>(constant)
            ->getValueAPF()
            .convertToDouble();;
        return *reinterpret_cast<value_t *>(&value_double);
      }

      case llvm::Type::PointerTyID: {
        if (constant->getNumOperands() > 0) {
          if (auto *constant_int =
              llvm::dyn_cast<llvm::ConstantInt>(constant->getOperand(0))) {
            return reinterpret_cast<value_t>(constant_int->getZExtValue());
          }
        }

        // fallthrough
      }

      default:
        throw NotSupportedException("unsupported constant type: " + CodeGen::Print(constant->getType()));
    }
  }
}

ContextBuilder::value_index_t ContextBuilder::AddConstant(
    const llvm::Constant *constant) {
  auto value_mapping_result = value_mapping_.find(constant);
  if (value_mapping_result != value_mapping_.end())
    return value_mapping_result->second;

  value_t value = GetConstantValue(constant);
  value_index_t value_index;

  // check if entry with this value already exists
  auto constant_result = std::find_if( constants_.begin(), constants_.end(),
                          [value](const std::pair<value_t, value_index_t>& item){ return item.first == value;} );

  if (constant_result == constants_.end()) {
    value_index = CreateValueIndex(constant);
    constants_.emplace_back(std::make_pair(value, value_index));

    // constants liveness starts at program start
    value_liveness_[value_index].definition = 0;
  } else {
    value_index = constant_result->second;
    CreateValueAlias(constant, value_index);
  }

  return value_index;
};

index_t ContextBuilder::GetValueSlot(value_index_t value_index) const {
  PL_ASSERT(value_index < value_slots_.size());
  return value_slots_[value_index];
}

index_t ContextBuilder::GetValueSlot(const llvm::Value *value) const {
  return value_slots_[GetValueIndex(value)];
}

void ContextBuilder::AddValueDefinition(value_index_t value_index,
                                   ContextBuilder::instruction_index_t definition) {
  PL_ASSERT(value_liveness_[value_index].definition == valueLivenessUnknown);
  value_liveness_[value_index].definition = definition;
}

void ContextBuilder::AddValueUsage(value_index_t value_index,
                                   ContextBuilder::instruction_index_t usage) {
  if (value_liveness_[value_index].last_usage == valueLivenessUnknown)
    value_liveness_[value_index].last_usage = usage;
  else
    value_liveness_[value_index].last_usage = std::max(value_liveness_[value_index].last_usage, usage);
}

index_t ContextBuilder::GetTemporaryValueSlot(const llvm::BasicBlock *bb) {
  // new entry in map is created automatically if necessary
  number_temporary_values_[bb]++;

  number_temporary_value_slots_ = std::max(number_temporary_value_slots_, static_cast<size_t>(number_temporary_values_[bb]));
  return number_value_slots_ + number_temporary_values_[bb] - 1;
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

bool ContextBuilder::IsConstantValue(llvm::Value *value) const {
  auto *constant = llvm::dyn_cast<llvm::Constant>(value);
  return (constant != nullptr);
}

int64_t ContextBuilder::GetConstantIntegerValueSigned(llvm::Value *constant) const {
  return llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
}

uint64_t ContextBuilder::GetConstantIntegerValueUnsigned(llvm::Value *constant) const {
  return llvm::cast<llvm::ConstantInt>(constant)->getZExtValue();
}

bool ContextBuilder::BasicBlockIsRPOSucc(const llvm::BasicBlock *bb, const llvm::BasicBlock *succ) const {
  for (size_t i = 0; i < bb_reverse_post_order_.size() - 1; i++) {
    if (bb_reverse_post_order_[i] == bb && bb_reverse_post_order_[i + 1] == succ)
      return true;
  }

  return false;
}

void ContextBuilder::AnalyseFunction() {
  std::unordered_map<const llvm::BasicBlock *, index_t> bb_last_instruction_index;

  /* - determine liveness of all values
   * - merge values of noop instructions
   * - merge constants and create list of constants
   */

  // function arguments are already defined when the function starts
  for (auto &argument : llvm_function_->args()) {
    value_index_t  value_index = CreateValueIndex(&argument);
    // DEF: function arguments are already defines at function start
    AddValueDefinition(value_index, 0);
  }

  instruction_index_t instruction_index = 0;
  for (llvm::ReversePostOrderTraversal<const llvm::Function *>::rpo_iterator
           traversal_iterator = rpo_traversal_.begin();
       traversal_iterator != rpo_traversal_.end(); ++traversal_iterator) {
    const llvm::BasicBlock* bb = *traversal_iterator;

    // add basic block to rpo vector for pred/succ lookups
    bb_reverse_post_order_.push_back(bb);

    for (llvm::BasicBlock::const_iterator instr_iterator = bb->begin();
         instr_iterator != bb->end(); ++instr_iterator) {
      const llvm::Instruction *instruction = instr_iterator;

      bool is_phi = false;
      if (instruction->getOpcode() == llvm::Instruction::PHI)
        is_phi = true;

      // iterate operands of instruction
      for (llvm::Instruction::const_op_iterator
               op_iterator = instruction->op_begin();
           op_iterator != instruction->op_end(); ++op_iterator) {
        llvm::Value *operand = op_iterator->get();

        if (IsConstantValue(operand)) {
          // exception: the called function in a CallInst is also a constant
          // but we want to skip this one
          auto *call_instruction = llvm::dyn_cast<llvm::CallInst>(instruction);
          if (call_instruction != nullptr && call_instruction->getCalledFunction() == &*operand)
            continue;

          // lookup value index for constant or create a new one if needed
          value_index_t value_index = AddConstant(llvm::cast<llvm::Constant>(operand));

          // USE: extend liveness of constant value
          AddValueUsage(value_index, instruction_index);

        // USE: extend liveness of operand value (if not phi operand)
        // A BasicBlock may be a label operand, but we don't need to track them
        } else if (!is_phi && (llvm::dyn_cast<llvm::BasicBlock>(operand) == nullptr)) {
          value_index_t operand_index = GetValueIndex(operand);
          AddValueUsage(operand_index, instruction_index);
        }
      }

      // DEF: save the instruction index as the liveness starting point

      // for some instructions we know in advance that they will produce a nop,
      // so we merge their value and their operand here
      if (instruction->getOpcode() == llvm::Instruction::BitCast ||
          instruction->getOpcode() == llvm::Instruction::Trunc ||
          instruction->getOpcode() == llvm::Instruction::PtrToInt ||
          (instruction->getOpcode() == llvm::Instruction::GetElementPtr && llvm::dyn_cast<llvm::GetElementPtrInst>(instruction)->hasAllZeroIndices())) {
        // merge operand resulting value
        CreateValueAlias(instruction,
                         GetValueIndex(instruction->getOperand(0)));

      } else if (!instruction->getType()->isVoidTy()) {
        value_index_t value_index = CreateValueIndex(instruction);
        AddValueDefinition(value_index, instruction_index);

      }

      ++instruction_index;
    }

    bb_last_instruction_index[bb] = instruction_index - 1;
  }

  instruction_index_max_ = instruction_index;

  // revisit phi nodes to extend the lifetime of their arguments if necessary
  // has to be done in second pass, as we need to know the last instruction
  // index for each basic block
  for (llvm::ReversePostOrderTraversal<const llvm::Function *>::rpo_iterator
           traversal_iterator = rpo_traversal_.begin();
       traversal_iterator != rpo_traversal_.end(); ++traversal_iterator) {
    const llvm::BasicBlock *bb = *traversal_iterator;
    for (llvm::BasicBlock::const_iterator instr_iterator = bb->begin();
         auto *phi_instruction = llvm::dyn_cast<llvm::PHINode>(&*instr_iterator); ++instr_iterator) {

      for (llvm::PHINode::const_block_iterator op_iterator = phi_instruction->block_begin();
           op_iterator != phi_instruction->block_end(); ++op_iterator) {
        llvm::BasicBlock *phi_bb = *op_iterator;
        llvm::Value *phi_value = phi_instruction->getIncomingValueForBlock(phi_bb);

        if (IsConstantValue(phi_value))
          continue;

        AddValueUsage(GetValueIndex(phi_value), bb_last_instruction_index[phi_bb]);
      }
    }
  }
}

void ContextBuilder::PerformNaiveRegisterAllocation() {
  // assign a value slot to every liveness range in value_liveness_
  value_slots_.resize(value_liveness_.size(), 0);

  // it is not worth removing entries from values that are never used,
  // so we simply skip them when iterating (if .last_usage = unknown)
  // and they have dummy slot 0 assigned

  index_t reg = 0;

  // iterate over other entries, which are already sorted
  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    // skip values that start at zero or that are never used
    if (value_liveness_[i].last_usage == valueLivenessUnknown)
      continue;

    value_slots_[i] = reg++ + 1; // + 1 because 0 is dummy slot
  }

  number_value_slots_ = reg + 1;
}

void ContextBuilder::PerformGreedyRegisterAllocation() {
  // assign a value slot to every liveness range in value_liveness_

  value_slots_.resize(value_liveness_.size(), 0);
  std::vector<ValueLiveness> registers;

  auto findEmptyRegister = [&registers](ValueLiveness liveness) {
    for (index_t i = 0; i < registers.size(); ++i) {
      if (registers[i].last_usage <= liveness.definition) {
        registers[i] = liveness;
        return i;
      }
    }

    // no empty register found, create new one
    registers.push_back(liveness);
    return static_cast<index_t>(registers.size() - 1);
  };

  // it is not worth removing entries from values that are never used,
  // so we simply skip them when iterating (if .last_usage = unknown)
  // and they have dummy slot 0 assigned

  // the vector value_liveness_ should already sorted by .definition
  // except for the constant values. so we just iterate it and pick out
  // the entries with .definition = 0 manually

  // get all entries with .definition = 0
  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    if (value_liveness_[i].definition == 0 && value_liveness_[i].last_usage != valueLivenessUnknown) {
      registers.push_back(value_liveness_[i]);
      value_slots_[i] = registers.size() - 1 + 1; // + 1 because 0 is dummy slot
    }
  }

#ifndef NDEBUG
  instruction_index_t instruction_index = 1;
#endif

  // iterate over other entries, which are already sorted
  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    // skip values that start at zero or that are never used
    if (value_liveness_[i].definition == 0 || value_liveness_[i].last_usage == valueLivenessUnknown)
      continue;

#ifndef NDEBUG
    PL_ASSERT(value_liveness_[i].definition >= instruction_index);
    instruction_index = value_liveness_[i].definition;
#endif

    value_slots_[i] = findEmptyRegister(value_liveness_[i]) + 1; // + 1 because 0 is dummy slot
  }

  number_value_slots_ = registers.size() + 1; // + 1 because 0 is dummy slot
}

void ContextBuilder::TranslateFunction() {
  std::unordered_map<const llvm::BasicBlock *, index_t> bb_mapping;
  std::vector<BytecodeRelocation> bytecode_relocations;

  for (llvm::ReversePostOrderTraversal<const llvm::Function *>::rpo_iterator
           traversal_iterator = rpo_traversal_.begin();
       traversal_iterator != rpo_traversal_.end(); ++traversal_iterator) {
    const llvm::BasicBlock* bb = *traversal_iterator;

    bb_mapping[bb] = context_.bytecode_.size();

    for (llvm::BasicBlock::const_iterator instr_iterator = bb->begin(); instr_iterator != bb->end(); ++instr_iterator) {
      const llvm::Instruction *instruction = instr_iterator;

      // DEBUG
      //LOG_DEBUG("Interpreter translating: %s\n", CodeGen::Print(instruction).c_str());

      switch (instruction->getOpcode()) {
        // Terminators
        case llvm::Instruction::Br:
          ProcessPHIsForBasicBlock(bb);
          TranslateBranch(instruction, bytecode_relocations);
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
          // values got already merged in analysis pass
          break;

        case llvm::Instruction::SExt:
        case llvm::Instruction::ZExt:
        case llvm::Instruction::IntToPtr:
          TranslateIntExt(instruction);
          break;

        case llvm::Instruction::Trunc:
        case llvm::Instruction::PtrToInt:
          // trunc translates to nop
          // values got already merged in analysis pass
          break;

        case llvm::Instruction::UIToFP:
        case llvm::Instruction::SIToFP:
        case llvm::Instruction::FPToUI:
        case llvm::Instruction::FPToSI:
          TranslateFloatIntCast(instruction);
          break;

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

  for (auto &relocation : bytecode_relocations) {
    reinterpret_cast<Instruction *>(&context_.bytecode_[relocation.instruction_slot])->args[relocation.argument] = bb_mapping[relocation.bb];
  }
}

void ContextBuilder::Finalize() {
  // calculate final number of value slots during runtime
  context_.number_values_ = number_value_slots_ + number_temporary_value_slots_;

  // check if number values exceeds bit range (unrealistic)
  if (context_.number_values_ >= std::numeric_limits<index_t>::max()) {
    throw NotSupportedException("number of values exceeds max number of bits");
  }

  // prepare constants for context
  context_.constants_.resize(constants_.size());
  for (size_t i = 0; i < constants_.size(); ++i) {
    context_.constants_[i] = std::make_pair(constants_[i].first, GetValueSlot(constants_[i].second));
  }

  // prepare arguments for context
  context_.function_arguments_.resize(llvm_function_->arg_size());
  index_t argument_index = 0;
  for (auto &argument : llvm_function_->args()) {
    context_.function_arguments_[argument_index++] = GetValueSlot(&argument);
  }
}

void ContextBuilder::ProcessPHIsForBasicBlock(const llvm::BasicBlock *bb) {
  typedef struct {
    const llvm::Instruction *instruction;
    index_t dest;
    index_t src;
  } AdditionalMove;

  std::vector<AdditionalMove> additional_moves;

  for (auto succ_iterator = llvm::succ_begin(bb); succ_iterator != llvm::succ_end(bb); ++succ_iterator) {
    // if the basic block is its own successor, we have to create additional
    // mov instructions (known as the phi swap problem)
    if (*succ_iterator == bb) {
      for (auto instruction_iterator = succ_iterator->begin(); auto *phi_node = llvm::dyn_cast<llvm::PHINode>(&*instruction_iterator); ++instruction_iterator) {
        index_t temp_slot = GetTemporaryValueSlot(bb);

        InsertBytecodeInstruction(phi_node,
                                  Opcode::phi_mov,
                                  temp_slot,
                                  GetValueSlot(phi_node->getIncomingValueForBlock(
                                      bb)));
        additional_moves.push_back({phi_node, GetValueSlot(phi_node), temp_slot});
      }
    // common case: create mov instruction to destination slot
    } else {
      for (auto instruction_iterator = succ_iterator->begin(); auto *phi_node = llvm::dyn_cast<llvm::PHINode>(&*instruction_iterator); ++instruction_iterator) {
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

void ContextBuilder::TranslateBranch(const llvm::Instruction *instruction, std::vector<BytecodeRelocation> &bytecode_relocations) {
  auto *branch_instruction = llvm::cast<llvm::BranchInst>(&*instruction);

  // conditional branch
  if (branch_instruction->isConditional()) {
    // The first operand is the false branch, while the second one
    // is the true one (printed llvm assembly is the other way round)
    // To be consistent, we use the order of the memory representation
    // in the bytecode.

    if (BasicBlockIsRPOSucc(branch_instruction->getParent(), llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(1)))) {
      InsertBytecodeInstruction(instruction,
                                Opcode::branch_cond_ft,
                                GetValueSlot(branch_instruction->getOperand(0)), 0);

      BytecodeRelocation relocation_false{static_cast<index_t>(context_.bytecode_.size() - 1),
                                          1,
                                          llvm::cast<llvm::BasicBlock>(
                                              branch_instruction->getOperand(2))};
      bytecode_relocations.push_back(relocation_false);
    } else {
      InsertBytecodeInstruction(instruction,
                                Opcode::branch_cond,
                                GetValueSlot(branch_instruction->getOperand(0)), 0, 0);

      BytecodeRelocation relocation_false{static_cast<index_t>(context_.bytecode_.size() - 1),
                                          1,
                                          llvm::cast<llvm::BasicBlock>(
                                              branch_instruction->getOperand(1))};
      bytecode_relocations.push_back(relocation_false);
      BytecodeRelocation relocation_true{static_cast<index_t>(context_.bytecode_.size() - 1),
                                         2,
                                         llvm::cast<llvm::BasicBlock>(
                                             branch_instruction->getOperand(2))};
      bytecode_relocations.push_back(relocation_true);
    }
  // unconditional branch
  } else {
    if (!BasicBlockIsRPOSucc(branch_instruction->getParent(), llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(0)))) {
      InsertBytecodeInstruction(instruction, Opcode::branch_uncond, 0);

      BytecodeRelocation relocation{static_cast<index_t>(context_.bytecode_.size() - 1),
                                    0,
                                    llvm::cast<llvm::BasicBlock>(
                                        branch_instruction->getOperand(0))};
      bytecode_relocations.push_back(relocation);
    }
  }
}

void ContextBuilder::TranslateReturn(const llvm::Instruction *instruction) {
  auto *return_instruction = llvm::cast<llvm::ReturnInst>(&*instruction);

  index_t return_slot = 0;
  if (return_instruction->getNumOperands() > 0)
    return_slot = GetValueSlot(return_instruction->getOperand(0));

  InsertBytecodeInstruction(instruction, Opcode::ret, return_slot);
}

void ContextBuilder::TranslateBinaryOperator(const llvm::Instruction *instruction) {
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

void ContextBuilder::TranslateAlloca(const llvm::Instruction *instruction) {
  auto *alloca_instruction = llvm::cast<llvm::AllocaInst>(&*instruction);
  Opcode opcode;

  // get type to allocate
  llvm::Type *type = alloca_instruction->getAllocatedType();

  // get type size in bytes
  size_t type_size = code_context_.GetTypeSize(type);

  if (alloca_instruction->isArrayAllocation()) {
    index_t array_size = GetValueSlot(alloca_instruction->getArraySize());
    opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::alloca_array), alloca_instruction->getArraySize()->getType());

    // type size is immediate value!
    InsertBytecodeInstruction(instruction, opcode,
                              GetValueSlot(alloca_instruction),
                              static_cast<index_t>(type_size),
                              array_size);
  } else {
    opcode = Opcode::alloca;
    // type size is immediate value!
    InsertBytecodeInstruction(instruction, opcode,
                              GetValueSlot(alloca_instruction),
                              static_cast<index_t>(type_size));
  }
}

void ContextBuilder::TranslateLoad(const llvm::Instruction *instruction) {
  auto *load_instruction = llvm::cast<llvm::LoadInst>(&*instruction);

  Opcode opcode = GetOpcodeForTypeSizeIntTypes(GET_FIRST_INT_TYPES(Opcode::load), load_instruction->getType());
  InsertBytecodeInstruction(instruction,
                            opcode,
                            load_instruction,
                            load_instruction->getPointerOperand());
}

void ContextBuilder::TranslateStore(const llvm::Instruction *instruction) {
  auto *store_instruction = llvm::cast<llvm::StoreInst>(&*instruction);

  Opcode opcode = GetOpcodeForTypeSizeIntTypes(GET_FIRST_INT_TYPES(Opcode::store), store_instruction->getOperand(0)->getType());
  InsertBytecodeInstruction(instruction,
                            opcode,
                            store_instruction->getPointerOperand(),
                            store_instruction->getValueOperand());
}

void ContextBuilder::TranslateGetElementPtr(const llvm::Instruction *instruction) {
  auto *gep_instruction = llvm::cast<llvm::GetElementPtrInst>(&*instruction);
  int64_t overall_offset = 0;

  // If the GEP translates to a nop, the values have been already merged
  // during the analysis pass
  if (gep_instruction->hasAllZeroIndices())
    return;

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

  // make sure that resulting type is correct
  PL_ASSERT(type == gep_instruction->getResultElementType());

  // fill in calculated overall offset in previously placed gep_offset
  // bytecode instruction
  gep_offset_bytecode_instruction.args[2] = static_cast<index_t>(overall_offset);
}

void ContextBuilder::TranslateFloatIntCast(const llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  Opcode opcode = Opcode::undefined;

  if (instruction->getOpcode() == llvm::Instruction::FPToSI) {
    if (cast_instruction->getOperand(0)->getType() == code_context_.float_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::floattosi), cast_instruction->getType());
    else if (cast_instruction->getOperand(0)->getType() == code_context_.double_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::doubletosi), cast_instruction->getType());
    else
      throw NotSupportedException("unsupported cast instruction");

  } else if (instruction->getOpcode() == llvm::Instruction::FPToUI) {
    if (cast_instruction->getOperand(0)->getType() == code_context_.float_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::floattoui), cast_instruction->getType());
    else if (cast_instruction->getOperand(0)->getType() == code_context_.double_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::doubletoui), cast_instruction->getType());
    else
      throw NotSupportedException("unsupported cast instruction");

  } else if (instruction->getOpcode() == llvm::Instruction::SIToFP) {
    if (cast_instruction->getType() == code_context_.float_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sitofloat), cast_instruction->getOperand(0)->getType());
    else if (cast_instruction->getType() == code_context_.double_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sitodouble), cast_instruction->getOperand(0)->getType());
    else
      throw NotSupportedException("unsupported cast instruction");

  } else if (instruction->getOpcode() == llvm::Instruction::UIToFP) {
    if (cast_instruction->getType() == code_context_.float_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::uitofloat), cast_instruction->getOperand(0)->getType());
    else if (cast_instruction->getType() == code_context_.double_type_)
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::uitodouble), cast_instruction->getOperand(0)->getType());
    else
      throw NotSupportedException("unsupported cast instruction");

  } else {
    throw NotSupportedException("unsupported cast instruction");
  }

  InsertBytecodeInstruction(cast_instruction, opcode, cast_instruction, cast_instruction->getOperand(0));
}

void ContextBuilder::TranslateIntExt(const llvm::Instruction *instruction) {
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

void ContextBuilder::TranslateCmp(const llvm::Instruction *instruction) {
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
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_ne), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGT:
    case llvm::CmpInst::Predicate::FCMP_OGT:
    case llvm::CmpInst::Predicate::FCMP_UGT:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_gt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGE:
    case llvm::CmpInst::Predicate::FCMP_OGE:
    case llvm::CmpInst::Predicate::FCMP_UGE:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_ge), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULT:
    case llvm::CmpInst::Predicate::FCMP_OLT:
    case llvm::CmpInst::Predicate::FCMP_ULT:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_lt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULE:
    case llvm::CmpInst::Predicate::FCMP_OLE:
    case llvm::CmpInst::Predicate::FCMP_ULE:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_le), type);
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

void ContextBuilder::TranslateCall(const llvm::Instruction *instruction) {
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

      context_.external_call_contexts_.push_back(call_context);

      InsertBytecodeExternalCallInstruction(call_instruction,
                                            static_cast<index_t>(
                                                context_.external_call_contexts_.size() - 1),
                                            raw_pointer);
    }
  } else {
    // internal function call to another IR function

    index_t dest_slot = 0;
    if (!instruction->getType()->isVoidTy())
      dest_slot = GetValueSlot(call_instruction);

    index_t sub_context_index;
    const auto result = sub_context_mapping_.find(function);
    if (result != sub_context_mapping_.end()) {
      sub_context_index = result->second;
    } else {
      auto sub_context = ContextBuilder::CreateInterpreterContext(code_context_, function);

      context_.sub_contexts_.push_back(std::move(sub_context));
      sub_context_index = context_.sub_contexts_.size() - 1;
      sub_context_mapping_[function] = sub_context_index;
    }

    InternalCallInstruction &bytecode_instruction = InsertBytecodeInternalCallInstruction(call_instruction, sub_context_index, dest_slot, call_instruction->getNumArgOperands());

    for (unsigned int i = 0; i < call_instruction->getNumArgOperands(); i++) {
      bytecode_instruction.args[i] = GetValueSlot(call_instruction->getArgOperand(i));

      if (code_context_.GetTypeSize(call_instruction->getArgOperand(i)->getType()) > 8) {
        throw NotSupportedException("argument for internal call too big");
      }
    }
  }
}

void ContextBuilder::TranslateSelect(const llvm::Instruction *instruction) {
  auto *select_instruction = llvm::cast<llvm::SelectInst>(&*instruction);

  InsertBytecodeInstruction<2>(select_instruction,
                               Opcode::select,
                               GetValueSlot(select_instruction),
                               GetValueSlot(select_instruction->getCondition()),
                               GetValueSlot(select_instruction->getTrueValue()),
                               GetValueSlot(select_instruction->getFalseValue()));
}

void ContextBuilder::TranslateExtractValue(const llvm::Instruction *instruction) {
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
