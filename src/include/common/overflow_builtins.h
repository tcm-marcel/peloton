//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_interpreter.h
//
// Identification: src/include/codegen/interpreter/overflow_builtins.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>

namespace peloton {
namespace codegen {
namespace interpreter {

//----------------------------------------------------------------------------//
// Fall back implementations if the gcc overflow builtins are not available.
//
// Documentation:
// https://gcc.gnu.org/onlinedocs/gcc/Integer-Overflow-Builtins.html
//----------------------------------------------------------------------------//

// No need to define the macros inside the namespace, but the source
// validator checks for these tags  so we can't omit them anyway.

#ifndef __builtin_add_overflow
template <typename type_t>
static inline bool __builtin_add_overflow(type_t a, type_t b, type_t *res) {
  *res = a + b;

  if (a >= 0 && std::numeric_limits<type_t>::max() - a < b) {
    return true;
  } else if (a < 0 && std::numeric_limits<type_t>::max() - a > b) {
    return true;
  }

  return false;
}
#endif

#ifndef __builtin_sub_overflow
template <typename type_t>
static inline bool __builtin_sub_overflow(type_t a, type_t b, type_t *res) {
  *res = a - b;

  if (a >= 0 && std::numeric_limits<type_t>::max() - a > b) {
    return true;
  } else if (a < 0 && std::numeric_limits<type_t>::max() - a < b) {
    return true;
  }

  return false;
}
#endif

#ifndef __builtin_mul_overflow
template <typename type_t>
static inline bool __builtin_mul_overflow(type_t a, type_t b, type_t *res) {
  *res = a * b;

  if (a != 0 && *res / a != b) {
    return true;
  }

  return false;
}
#endif

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
