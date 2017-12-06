//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// map_stack_frame.cpp
//
// Identification: src/codegen/interpreter/map_stack_frame.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <llvm/IR/DataLayout.h>
#include "codegen/interpreter/map_stack_frame.h"

namespace peloton {
namespace codegen {
namespace interpreter {

MapStackFrame::MapStackFrame(const CodeContext &context) : context_(context) {}

uintptr_t MapStackFrame::Alloca(size_t size, size_t alignment) {
  (void)alignment;

  // allocate memory
  std::unique_ptr<char[]> pointer = std::make_unique<char[]>(size);

  // get raw pointer before moving pointer object!
  uintptr_t raw_pointer = reinterpret_cast<uintptr_t>(pointer.get());

  allocations_.emplace_back(std::move(pointer));
  return raw_pointer;
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
