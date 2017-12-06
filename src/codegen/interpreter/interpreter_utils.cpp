//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_utils.cpp
//
// Identification: src/codegen/interpreter/interpreter_utils.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/interpreter_utils.h"
#include <ffi.h>
#include <include/codegen/compilation_context.h>

namespace peloton {
namespace codegen {
namespace interpreter {

ffi_type *InterpreterUtils::GetFFIType(const CodeContext &context,
                                       llvm::Type *type) {
  if (type->isVoidTy()) {
    return &ffi_type_void;
  } else if (type->isPointerTy()) {
    return &ffi_type_pointer;
  }

  // exact type not necessary, only size is important
  switch (context.GetTypeSize(type)) {
    case 1: {
      return &ffi_type_uint8;
    }

    case 2: {
      return &ffi_type_uint16;
    }

    case 4: {
      return &ffi_type_uint32;
    }

    case 8: {
      return &ffi_type_uint64;
    }

    default: {
      throw Exception(
          std::string("unsupported argument size for external function call"));
    }
  }
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
