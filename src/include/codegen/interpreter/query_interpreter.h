//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.h
//
// Identification: src/include/codegen/interpreter/query_interpreter.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/interpreter/interpreter_context.h"
#include "codegen/query.h"
#include "include/common/exception.h"

namespace peloton {
namespace codegen {
namespace interpreter {

typedef struct {
  ffi_cif call_interface;
  std::vector<value_t *> value_pointers;
  value_t *return_pointer;
} CallActivation;


class QueryInterpreter {
 public:
  explicit QueryInterpreter(const InterpreterContext &context);

  static value_t ExecuteFunction(const InterpreterContext &context, const std::vector<value_t> &arguments);
  static void ExecuteFunction(const InterpreterContext &context, char *param);

 private:
  void ExecuteFunction(const std::vector<value_t> &arguments);
  void InitializeActivationRecord(const std::vector<value_t> &arguments);

  template <typename type_t>
  type_t GetReturnValue();

  template <typename type_t>
  ALWAYS_INLINE inline type_t GetValue(const index_t index) {
    PL_ASSERT(index >= 0 && index < context_.number_values_);
    return *reinterpret_cast<type_t *>(&values_[index]);
  }

  template <typename type_t>
  ALWAYS_INLINE inline type_t &GetValueReference(const index_t index) {
    PL_ASSERT(index >= 0 && index < context_.number_values_);
    return reinterpret_cast<type_t &>(values_[index]);
  }

  template <typename type_t>
  ALWAYS_INLINE inline void SetValue(const index_t index, const type_t value) {
    PL_ASSERT(index >= 0 && index < context_.number_values_);
    *reinterpret_cast<type_t *>(&values_[index]) = value;

#ifdef LOG_TRACE_ENABLED
    std::ostringstream output;
    output << "  [" << std::dec << std::setw(3) << index << "] <= " << value << "/0x" << std::hex << value;
    LOG_TRACE("%s", output.str().c_str());
#endif
  }

  template <size_t number_instruction_slots>
  ALWAYS_INLINE inline const Instruction *AdvanceIP(const Instruction *instruction) {
    auto next = reinterpret_cast<const Instruction *>(const_cast<instr_slot_t *>(reinterpret_cast<const instr_slot_t *>(instruction)) + number_instruction_slots);
    return next;
  }

  ALWAYS_INLINE inline const Instruction *AdvanceIP(const Instruction *instruction, size_t number_instruction_slots) {
    auto next = reinterpret_cast<const Instruction *>(const_cast<instr_slot_t *>(reinterpret_cast<const instr_slot_t *>(instruction)) + number_instruction_slots);
    return next;
  }

  uintptr_t AllocateMemory(size_t number_bytes);


  //--------------------------------------------------------------------------//

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *addHandler(const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) + GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *subHandler(const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) - GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *mulHandler(const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) * GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }



  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *divHandler(const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) / GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sdivHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0], (GetValue<type_signed_t>(instruction->args[1]) / GetValue<type_signed_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uremHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) % GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *fremHandler(const Instruction *instruction) {
    static_assert(std::is_floating_point<type_t>::value, "__func__ must only be used with floating point types");
    SetValue<type_t>(instruction->args[0], (std::fmod(GetValue<type_t>(instruction->args[1]), GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sremHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0], (GetValue<type_signed_t>(instruction->args[1]) % GetValue<type_signed_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }



  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *shlHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) << GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *lshrHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) >> GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *ashrHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0], (GetValue<type_signed_t>(instruction->args[1]) >> GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *andHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) & GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *orHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) | GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *xorHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0], (GetValue<type_t>(instruction->args[1]) ^ GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }



  ALWAYS_INLINE inline const Instruction *extractvalueHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (GetValue<value_t>(instruction->args[1]) >> instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *loadHandler(const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0], (*GetValue<type_t *>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *storeHandler(const Instruction *instruction) {
    *GetValue<type_t *>(instruction->args[0]) = GetValue<type_t>(instruction->args[1]);
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *alloca_arrayHandler(const Instruction *instruction) {
    size_t number_bytes = instruction->args[1] * GetValue<type_t>(instruction->args[2]);
    SetValue<uintptr_t>(instruction->args[0], (AllocateMemory(number_bytes)));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *allocaHandler(const Instruction *instruction) {
    size_t number_bytes = instruction->args[1];
    SetValue<uintptr_t>(instruction->args[0], (AllocateMemory(number_bytes)));
    return AdvanceIP<1>(instruction);
  }



  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_eqHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) == GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_neHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) != GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_gtHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) > GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_ltHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) < GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_geHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) >= GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_leHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) <= GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sgtHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) > GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sltHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) < GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sgeHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) >= GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sleHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(instruction->args[0], (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) <= GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }



  ALWAYS_INLINE inline const Instruction *sext_i8_i16Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i16>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i8_i32Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i32>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i8_i64Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i16_i32Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i16>::type;
    using dest_t = typename std::make_signed<i32>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i16_i64Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i16>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i32_i64Handler(const Instruction *instruction) {
    using src_t = typename std::make_signed<i32>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i16Handler(const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i16;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i32Handler(const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i32;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i64Handler(const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i64;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i16_i32Handler(const Instruction *instruction) {
    using src_t = i16;
    using dest_t = i32;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i16_i64Handler(const Instruction *instruction) {
    using src_t = i16;
    using dest_t = i64;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i32_i64Handler(const Instruction *instruction) {
    using src_t = i32;
    using dest_t = i64;
    SetValue<dest_t>(instruction->args[0], (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }


  // The FP<>Int casts are created in a two-level hierarchy
  // eg. the generated call to floattosiHandler<i8> is redirected to tosiHandler<float, i8>

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *tosiHandler(const Instruction *instruction) {
    static_assert(std::is_integral<dest_type_t>::value, "__func__ dest_type must be an integer type");
    static_assert(std::is_floating_point<src_type_t>::value, "__func__ src_type must be a floating point type");
    using dest_type_signed_t = typename std::make_signed<dest_type_t>::type;

    SetValue<dest_type_signed_t>(instruction->args[0], (static_cast<dest_type_signed_t>(GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *touiHandler(const Instruction *instruction) {
    static_assert(std::is_integral<dest_type_t>::value, "__func__ dest_type must be an integer type");
    static_assert(std::is_floating_point<src_type_t>::value, "__func__ src_type must be a floating point type");

    SetValue<dest_type_t>(instruction->args[0], (static_cast<dest_type_t>(GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *sitoHandler(const Instruction *instruction) {
    static_assert(std::is_floating_point<dest_type_t>::value, "__func__ dest_type must be a floating point type");
    static_assert(std::is_integral<src_type_t>::value, "__func__ src_type must be an integer type");
    using src_type_signed_t = typename std::make_signed<src_type_t>::type;

    SetValue<dest_type_t>(instruction->args[0], (static_cast<dest_type_t>(GetValue<src_type_signed_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *uitoHandler(const Instruction *instruction) {
    static_assert(std::is_floating_point<dest_type_t>::value, "__func__ dest_type must be a floating point type");
    static_assert(std::is_integral<src_type_t>::value, "__func__ src_type must be an integer type");

    SetValue<dest_type_t>(instruction->args[0], (static_cast<dest_type_t>(GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }



  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *floattosiHandler(const Instruction *instruction) {
    return tosiHandler<float, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *floattouiHandler(const Instruction *instruction) {
    return touiHandler<float, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sitofloatHandler(const Instruction *instruction) {
    return sitoHandler<type_t, float>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uitofloatHandler(const Instruction *instruction) {
    return uitoHandler<type_t, float>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *doubletosiHandler(const Instruction *instruction) {
    return tosiHandler<double, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *doubletouiHandler(const Instruction *instruction) {
    return touiHandler<double, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sitodoubleHandler(const Instruction *instruction) {
    return sitoHandler<type_t, double>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uitodoubleHandler(const Instruction *instruction) {
    return uitoHandler<type_t, double>(instruction);
  }





  ALWAYS_INLINE inline const Instruction *gep_offsetHandler(const Instruction *instruction) {
    uintptr_t sum = GetValue<uintptr_t>(instruction->args[1]) + instruction->args[2];
    SetValue<uintptr_t>(instruction->args[0], (sum));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *gep_arrayHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    uintptr_t product = GetValue<type_t>(instruction->args[1]) * instruction->args[2];
    SetValue<uintptr_t>(instruction->args[0], (GetValue<uintptr_t>(instruction->args[0]) + product));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *phi_movHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (GetValue<value_t>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *selectHandler(const Instruction *instruction) {
    value_t result;
    if (GetValue<i8>(instruction->args[1]) > 0)
      result = GetValue<value_t>(instruction->args[2]);
    else
      result = GetValue<value_t>(instruction->args[3]);

    SetValue<value_t>(instruction->args[0], (result));
    return AdvanceIP<2>(instruction); // bigger slot size!
  }

  ALWAYS_INLINE inline const Instruction *call_externalHandler(const Instruction *instruction) {
    const ExternalCallInstruction *call_instruction= reinterpret_cast<const ExternalCallInstruction *>(instruction);
    CallActivation &call_activation = call_activations_[call_instruction->external_call_context];

    // call external function
    ffi_call(&call_activation.call_interface, call_instruction->function,
             call_activation.return_pointer, reinterpret_cast<void **>(call_activation.value_pointers.data()));

    // DEBUG
#ifdef LOG_TRACE_ENABLED
    if (context_.external_call_contexts_[call_instruction->external_call_context].dest_type != &ffi_type_void) {
      std::ostringstream output;
      value_t value =
          GetValue<value_t>(context_.external_call_contexts_[call_instruction->external_call_context].dest_slot);
      output << " -> [" << std::dec << std::setw(3)
             << context_.external_call_contexts_[call_instruction->external_call_context].dest_slot
             << "] = " << value << " / 0x" << std::hex << value << ",";
      LOG_TRACE("%s\n", output.str().c_str());
    }
#endif

    return AdvanceIP<2>(instruction); // bigger slot size!
  }

  ALWAYS_INLINE inline const Instruction *call_internalHandler(const Instruction *instruction) {
    const InternalCallInstruction *call_instruction = reinterpret_cast<const InternalCallInstruction *>(instruction);

    std::vector<value_t> arguments(call_instruction->number_args);
    for (size_t i = 0; i < call_instruction->number_args; i++) {
      arguments[i] = GetValue<value_t>(call_instruction->args[i]);
    }

    value_t result = ExecuteFunction(context_.sub_contexts_[call_instruction->sub_context], arguments);
    SetValue(call_instruction->dest_slot, result);

    return AdvanceIP(instruction, context_.GetInteralCallInstructionSlotSize(call_instruction));
  }

  ALWAYS_INLINE inline const Instruction *nop_movHandler(const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0], (GetValue<value_t>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }




  ALWAYS_INLINE inline const Instruction *branch_uncondHandler(const Instruction *instruction) {
    return context_.GetIPFromIndex(instruction->args[0]);
  }

  ALWAYS_INLINE inline const Instruction *branch_condHandler(const Instruction *instruction) {
    index_t next_bb;
    if (GetValue<i8>(instruction->args[0]) > 0)
      next_bb = instruction->args[2];
    else
      next_bb = instruction->args[1];

    return context_.GetIPFromIndex(next_bb);
  }

  ALWAYS_INLINE inline const Instruction *branch_cond_ftHandler(const Instruction *instruction) {
    const Instruction *ip;
    if ((GetValue<value_t>(instruction->args[0]) & 0x1) > 0)
      ip = context_.GetIPFromIndex(instruction->args[1]);
    else
      ip = AdvanceIP<1>(instruction);

    return ip;
  }



  ALWAYS_INLINE inline const Instruction *llvm_memcpyHandler(const Instruction *instruction) {
    PL_MEMCPY(
        GetValue<void *>(instruction->args[0]),
        GetValue<void *>(instruction->args[1]),
        GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_memmoveHandler(const Instruction *instruction) {
    std::memmove(
        GetValue<void *>(instruction->args[0]),
        GetValue<void *>(instruction->args[1]),
        GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_memsetHandler(const Instruction *instruction) {
    PL_MEMSET(
        GetValue<void *>(instruction->args[0]),
        GetValue<i8>(instruction->args[1]),
        GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }


  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_uadd_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    bool overflow = __builtin_add_overflow(GetValue<type_t>(instruction->args[2]), GetValue<type_t>(instruction->args[3]), &GetValueReference<type_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_sadd_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_add_overflow(GetValue<type_signed_t>(instruction->args[2]), GetValue<type_signed_t>(instruction->args[3]), &GetValueReference<type_signed_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_usub_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    bool overflow = __builtin_sub_overflow(GetValue<type_t>(instruction->args[2]), GetValue<type_t>(instruction->args[3]), &GetValueReference<type_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_ssub_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_sub_overflow(GetValue<type_signed_t>(instruction->args[2]), GetValue<type_signed_t>(instruction->args[3]), &GetValueReference<type_signed_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_umul_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    bool overflow = __builtin_mul_overflow(GetValue<type_t>(instruction->args[2]), GetValue<type_t>(instruction->args[3]), &GetValueReference<type_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_smul_overflowHandler(const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value, "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_mul_overflow(GetValue<type_signed_t>(instruction->args[2]), GetValue<type_signed_t>(instruction->args[3]), &GetValueReference<type_signed_t>(instruction->args[0]));
    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_sse42_crc32Handler(const Instruction *instruction) {
    SetValue<i64>(instruction->args[0], (__builtin_ia32_crc32di(GetValue<i64>(instruction->args[1]), GetValue<i64>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  //--------------------------------------------------------------------------//

 private:
  static void *label_pointers_[InterpreterContext::GetNumberOpcodes()];

  // 64 Byte is common for cache lines
  alignas(64) std::vector<value_t> values_;
  std::vector<std::unique_ptr<char[]>> allocations_;
  std::vector<CallActivation> call_activations_;

  const InterpreterContext &context_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(QueryInterpreter);
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
