//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_utils.h
//
// Identification: src/include/codegen/interpreter/interpreter_utils.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "include/common/exception.h"
#include <ffi.h>

#include <llvm/Support/raw_ostream.h>

namespace peloton {
namespace codegen {
namespace interpreter {

class InterpreterUtils {
 public:
  // The interpreter assumes that all values fit into this type
  using value_t = uint64_t;
  using value_signed_t = std::make_signed<value_t>::type;

  //--------------------------------------------------------------------------//
  // Helper Functions
  //--------------------------------------------------------------------------//

  // Mask value according to given size, setting the unused part to zero
  // size given in bytes, as returned from CompilationContext::GetTypeSize
  static value_t MaskValue(value_t value, size_t size) ALWAYS_INLINE {
    // skip if nothing to mask
    if (size == sizeof(value_t)) return value;

    value_t mask = ((value_t)1 << (size * 8)) - 1;
    return value & mask;
  }

  // Extend signed values from value_t to actually perform arithmetics with them
  static value_signed_t ExtendSignedValue(value_t value, size_t size_old,
                                          size_t size_new = sizeof(value_t))
      ALWAYS_INLINE {
    PL_ASSERT(size_old <= size_new);

    // mask to old size
    value = MaskValue(value, size_old);

    // skip if size doesn't change (masks don't work for that case!)
    if (size_old == size_new)
      return *reinterpret_cast<value_signed_t *>(&value);

    // check if MSB of old value is one
    if ((value & (1 << (size_old * 8 - 1))) > 0) {
      // if yes, fill remainder with ones (to preserve two's complement)
      value = value | ~(((value_t)1 << (size_old * 8)) - 1);
    }

    // mask to new size
    value = MaskValue(value, size_new);

    // be careful with simple assignment! we need a reinterpret_cast
    return *reinterpret_cast<value_signed_t *>(&value);
  }

  // Shrink signed values back into value_t
  static value_t ShrinkSignedValue(value_signed_t value,
                                   size_t size_new) ALWAYS_INLINE {
    value_t value_casted = *reinterpret_cast<value_t *>(&value);

    // shrinking is easy, we just have to mask
    value_casted = MaskValue(value_casted, size_new);

    // be careful with simple assignment! we need a reinterpret_cast
    return value_casted;
  }

  // Get the actual value of a llvm Constant (call by reference)
  static value_t GetConstantValue(llvm::Constant *constant) {
    llvm::Type *type = constant->getType();

    switch (type->getTypeID()) {
      case llvm::Type::IntegerTyID: {
        return llvm::cast<llvm::ConstantInt>(constant)->getZExtValue();
      }

      case llvm::Type::FloatTyID: {
        value_t value;
        *reinterpret_cast<float *>(&value) =
            llvm::cast<llvm::ConstantFP>(constant)
                ->getValueAPF()
                .convertToFloat();
        return value;
      }

      case llvm::Type::DoubleTyID: {
        value_t value;
        *reinterpret_cast<double *>(&value) =
            llvm::cast<llvm::ConstantFP>(constant)
                ->getValueAPF()
                .convertToDouble();
        return value;
      }

      case llvm::Type::PointerTyID: {
        if (auto *constant_int =
                llvm::dyn_cast<llvm::ConstantInt>(constant->getOperand(0))) {
          return constant_int->getZExtValue();
        }

        // fallthrough
      }

      default:
        std::string string;
        llvm::raw_string_ostream llvm_stream(string);
        llvm_stream << "unsupported llvm::Constant type: " << *constant;
        throw Exception(llvm_stream.str().c_str());
    }
  }

  // MemCopy function for Values, where the size is not known at compile time
  // This function exists because std::memcpy can not be inlined
  static inline void MemCopy(void *dest, void *src,
                             size_t count) ALWAYS_INLINE {
    switch (count) {
      case 1: {
        *reinterpret_cast<uint8_t *>(dest) = *reinterpret_cast<uint8_t *>(src);
      }
      case 2: {
        *reinterpret_cast<uint16_t *>(dest) =
            *reinterpret_cast<uint16_t *>(src);
      }
      case 4: {
        *reinterpret_cast<uint32_t *>(dest) =
            *reinterpret_cast<uint32_t *>(src);
      }
      case 8: {
        *reinterpret_cast<uint64_t *>(dest) =
            *reinterpret_cast<uint64_t *>(src);
      }
      default: { PL_MEMCPY(dest, src, count); }
    }
  }

  // DEBUG
  static std::string Print(const llvm::Value *value) {
    std::string string;
    llvm::raw_string_ostream llvm_stream(string);
    llvm_stream << *value;
    return llvm_stream.str();
  }

  static std::string Print(llvm::Type *type) {
    std::string string;
    llvm::raw_string_ostream llvm_stream(string);
    llvm_stream << *type;
    return llvm_stream.str();
  }

  // Get the ffi_type that fits the given llvm type
  static ffi_type *GetFFIType(const CodeContext &context, llvm::Type *type);
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
