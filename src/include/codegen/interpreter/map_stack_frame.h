//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// map_stack_frame.h
//
// Identification: src/include/codegen/interpreter/map_stack_frame.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "codegen/interpreter/interpreter_utils.h"

#include <llvm/IR/Value.h>
#include <unordered_map>
#include <memory>
#include <cstdint>

namespace peloton {
namespace codegen {
namespace interpreter {

// template <typename Allocator>
class MapStackFrame {
 public:
  //--------------------------------------------------------------------------//
  // Values are identified by the llvm::Value pointer
  // The actual values are passed around in value_t, which is 64bit and should
  // be able to hold every type that occurs in peloton. The unused bits inside
  // value_t are get zeroed when setting the value.
  // InterpreterUtils provides several methods to deal with those values.
  //--------------------------------------------------------------------------//

  explicit MapStackFrame(const CodeContext &context);

  // call by reference version of GetValue()
  // inline void GetValue(llvm::Value *identifier, void *destination)
  // ALWAYS_INLINE {
  InterpreterUtils::value_t GetValue(llvm::Value *identifier) {
    // Check if value is constant
    if (llvm::Constant *constant = llvm::dyn_cast<llvm::Constant>(identifier)) {
      return InterpreterUtils::GetConstantValue(constant);
    } else {
      // make sure the entry exists
      PL_ASSERT(llvm_values_.find(identifier) != llvm_values_.end());

      // get value
      return llvm_values_[identifier];
    }
  }

  // call by reference version of SetValue()
  // inline void SetValue(llvm::Value *identifier, void *source) ALWAYS_INLINE {
  void SetValue(llvm::Value *identifier, InterpreterUtils::value_t value) {
    PL_ASSERT(context_.GetTypeSize(identifier->getType()) <=
              sizeof(InterpreterUtils::value_t));

    // mask value TODO: too much overhead?
    value = InterpreterUtils::MaskValue(
        value, context_.GetTypeSize(identifier->getType()));

    // add to map or override existing value
    llvm_values_[identifier] = value;

    LOG_TRACE(" => %ld %lu 0x%016lX\n", value, value, value);
  }

  uintptr_t Alloca(size_t array_size, size_t alignment);

 private:
  const CodeContext &context_;

  std::unordered_map<llvm::Value *, InterpreterUtils::value_t> llvm_values_;
  std::vector<std::unique_ptr<char[]>> allocations_;
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
