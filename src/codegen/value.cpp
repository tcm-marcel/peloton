//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.cpp
//
// Identification: src/codegen/value.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value.h"

#include <list>
#include <queue>

#include "codegen/type/type_system.h"
#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {

Value::Value() : Value(type::Type{peloton::type::TypeId::INVALID, false}) {}

Value::Value(const type::Type &type, llvm::Value *val, llvm::Value *length,
             llvm::Value *null)
    : type_(type), value_(val), length_(length), null_(null) {
  // If the value is NULL-able, it better have an accompanying NULL bit
  PL_ASSERT(!type_.nullable || null_ != nullptr);
}

// Return a boolean value indicating whether this value is NULL
llvm::Value *Value::IsNull(CodeGen &codegen) const {
  if (IsNullable()) {
    PL_ASSERT(null_ != nullptr);
    return null_;
  } else {
    return codegen.ConstBool(false);
  }
}

// Return a boolean (i1) value indicating whether this value is not NULL
llvm::Value *Value::IsNotNull(CodeGen &codegen) const {
  return codegen->CreateNot(IsNull(codegen));
}

//===----------------------------------------------------------------------===//
// COMPARISONS
//===----------------------------------------------------------------------===//

Value Value::CastTo(CodeGen &codegen, const type::Type &to_type) const {
  // If the type we're casting to is the type of the value, we're done
  if (GetType() == to_type) {
    return *this;
  }

  // Do the explicit cast
  const auto *cast = type::TypeSystem::GetCast(GetType(), to_type);

  if (IsNullable()) {
    // We need to handle NULL's during casting
    type::TypeSystem::CastWithNullPropagation null_aware_cast{*cast};
    return null_aware_cast.DoCast(codegen, *this, to_type);
  } else {
    // No NULL, just do the cast
    PL_ASSERT(!to_type.nullable);
    return cast->DoCast(codegen, *this, to_type);
  }
}

#define DO_COMPARE(OP)                                                     \
  type::Type left_cast = GetType();                                        \
  type::Type right_cast = other.GetType();                                 \
                                                                           \
  const auto *comparison = type::TypeSystem::GetComparison(                \
      GetType(), left_cast, other.GetType(), right_cast);                  \
                                                                           \
  Value left = CastTo(codegen, left_cast);                                 \
  Value right = other.CastTo(codegen, right_cast);                         \
                                                                           \
  if (!left.IsNullable() && !right.IsNullable()) {                         \
    return comparison->Do##OP(codegen, left, right);                       \
  } else {                                                                 \
    type::TypeSystem::ComparisonWithNullPropagation null_aware_comparison{ \
        *comparison};                                                      \
    return null_aware_comparison.Do##OP(codegen, left, right);             \
  }

Value Value::CompareEq(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareEq);
}

Value Value::CompareNe(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareNe);
}

Value Value::CompareLt(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareLt);
}

Value Value::CompareLte(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareLte);
}

Value Value::CompareGt(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareGt);
}

Value Value::CompareGte(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(CompareGte);
}

Value Value::CompareForSort(CodeGen &codegen, const Value &other) const {
  DO_COMPARE(ComparisonForSort);
}

#undef DO_COMPARE

// Check that all the values from the left and equal to all the values in right
Value Value::TestEquality(CodeGen &codegen, const std::vector<Value> &lhs,
                          const std::vector<Value> &rhs) {
  std::queue<Value, std::list<Value>> results;
  // Perform the comparison of each element of lhs to rhs
  for (size_t i = 0; i < lhs.size(); i++) {
    results.push(lhs[i].CompareEq(codegen, rhs[i]));
  }

  // Tournament-style collapse
  while (results.size() > 1) {
    Value first = results.front();
    results.pop();
    Value second = results.front();
    results.pop();
    results.push(first.LogicalAnd(codegen, second));
  }
  return results.front();
}

//===----------------------------------------------------------------------===//
// ARITHMETIC OPERATIONS
//===----------------------------------------------------------------------===//

Value ExecBinaryOp(CodeGen &codegen, OperatorId op_id, const Value &left,
                   const Value &right, OnError on_error) {
  type::Type left_target_type = left.GetType();
  type::Type right_target_type = right.GetType();

  auto *binary_op = type::TypeSystem::GetBinaryOperator(
      op_id, left.GetType(), left_target_type, right.GetType(),
      right_target_type);

  Value casted_left = left.CastTo(codegen, left_target_type);
  Value casted_right = right.CastTo(codegen, right_target_type);

  // Check if we need to do a NULL-aware binary operation invocation
  if (!casted_left.IsNullable() && !casted_right.IsNullable()) {
    // Nope
    return binary_op->DoWork(codegen, casted_left, casted_right, on_error);
  } else {
    // One of the inputs are NULL
    type::TypeSystem::BinaryOperatorWithNullPropagation null_aware_bin_op{
        *binary_op};
    return null_aware_bin_op.DoWork(codegen, casted_left, casted_right,
                                    on_error);
  }
}

// Addition
Value Value::Add(CodeGen &codegen, const Value &other, OnError on_error) const {
  return ExecBinaryOp(codegen, OperatorId::Add, *this, other, on_error);
}

// Subtraction
Value Value::Sub(CodeGen &codegen, const Value &other, OnError on_error) const {
  return ExecBinaryOp(codegen, OperatorId::Sub, *this, other, on_error);
}

// Multiplication
Value Value::Mul(CodeGen &codegen, const Value &other, OnError on_error) const {
  return ExecBinaryOp(codegen, OperatorId::Mul, *this, other, on_error);
}

// Division
Value Value::Div(CodeGen &codegen, const Value &other, OnError on_error) const {
  return ExecBinaryOp(codegen, OperatorId::Div, *this, other, on_error);
}

// Modulus
Value Value::Mod(CodeGen &codegen, const Value &other, OnError on_error) const {
  return ExecBinaryOp(codegen, OperatorId::Mod, *this, other, on_error);
}

// Logical AND
Value Value::LogicalAnd(CodeGen &codegen, const Value &other) const {
  return ExecBinaryOp(codegen, OperatorId::LogicalAnd, *this, other,
                      OnError::Exception);
}

// Logical OR
Value Value::LogicalOr(CodeGen &codegen, const Value &other) const {
  return ExecBinaryOp(codegen, OperatorId::LogicalOr, *this, other,
                      OnError::Exception);
}

// TODO: Min/Max need to handle NULL

// Mathematical minimum
Value Value::Min(CodeGen &codegen, const Value &other) const {
  // Check if this < o
  auto is_lt = CompareLt(codegen, other);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_lt.GetValue(), GetValue(), other.GetValue());
  llvm::Value *len = nullptr;
  if (GetType().GetSqlType().IsVariableLength()) {
    len =
        codegen->CreateSelect(is_lt.GetValue(), GetLength(), other.GetLength());
  }
  return Value{GetType(), val, len};
}

// Mathematical maximum
Value Value::Max(CodeGen &codegen, const Value &other) const {
  // Check if this > other
  auto is_gt = CompareGt(codegen, other);

  // Choose either this or other depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_gt.GetValue(), GetValue(), other.GetValue());
  llvm::Value *len = nullptr;
  if (GetType().GetSqlType().IsVariableLength()) {
    len =
        codegen->CreateSelect(is_gt.GetValue(), GetLength(), other.GetLength());
  }
  return Value{GetType(), val, len};
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForHash(llvm::Value *&val, llvm::Value *&len) const {
  PL_ASSERT(GetType().type_id != peloton::type::TypeId::INVALID);
  val = GetValue();
  len = GetType().GetSqlType().IsVariableLength() ? GetLength() : nullptr;
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForMaterialization(CodeGen &codegen, llvm::Value *&val,
                                     llvm::Value *&len,
                                     llvm::Value *&null) const {
  PL_ASSERT(GetType().type_id != peloton::type::TypeId::INVALID);
  val = GetValue();
  len = GetType().GetSqlType().IsVariableLength() ? GetLength() : nullptr;
  null = IsNull(codegen);
}

// Return the value that can be
Value Value::ValueFromMaterialization(const type::Type &type, llvm::Value *val,
                                      llvm::Value *len, llvm::Value *null) {
  PL_ASSERT(type.type_id != peloton::type::TypeId::INVALID);
  return Value{type, val,
               (type.GetSqlType().IsVariableLength() ? len : nullptr),
               (type.nullable ? null : nullptr)};
}

// Build a new value that combines values arriving from different BB's into a
// single value.
Value Value::BuildPHI(
    CodeGen &codegen,
    const std::vector<std::pair<Value, llvm::BasicBlock *>> &vals) {
  uint32_t num_entries = static_cast<uint32_t>(vals.size());

  // The SQL type of the values that we merge here
  // TODO: Need to make sure all incoming types are unifyable
  const type::Type &type = vals[0].first.GetType();
  const type::SqlType &sql_type = type.GetSqlType();

  // Get the LLVM type for the values
  llvm::Type *null_type = codegen.BoolType();
  llvm::Type *val_type = nullptr, *len_type = nullptr;
  sql_type.GetTypeForMaterialization(codegen, val_type, len_type);
  PL_ASSERT(val_type != nullptr);

  // Do the merge depending on the type
  if (sql_type.IsVariableLength()) {
    PL_ASSERT(len_type != nullptr);
    auto *val_phi = codegen->CreatePHI(val_type, num_entries);
    auto *len_phi = codegen->CreatePHI(len_type, num_entries);
    auto *null_phi = codegen->CreatePHI(null_type, num_entries);
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      len_phi->addIncoming(val_pair.first.GetLength(), val_pair.second);
      null_phi->addIncoming(val_pair.first.IsNull(codegen), val_pair.second);
    }
    return Value{type, val_phi, len_phi, null_phi};
  } else {
    PL_ASSERT(len_type == nullptr);
    auto *val_phi = codegen->CreatePHI(val_type, num_entries);
    auto *null_phi = codegen->CreatePHI(null_type, num_entries);
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      null_phi->addIncoming(val_pair.first.IsNull(codegen), val_pair.second);
    }
    return Value{type, val_phi, nullptr, null_phi};
  }
}

}  // namespace codegen
}  // namespace peloton
