//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_test.cpp
//
// Identification: test/codegen/interpreter_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "include/storage/storage_manager.h"
#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "codegen/interpreter/query_interpreter.h"
#include "codegen/interpreter/interpreter_utils.h"
#include "concurrency/transaction_manager_factory.h"
#include "codegen/function_builder.h"

#include <llvm/IR/IRBuilder.h>

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

using namespace codegen;
using namespace interpreter;

class InterpreterTest : public PelotonCodeGenTest {
 public:
  using value_t = codegen::interpreter::InterpreterUtils::value_t;
  using value_signed_t = codegen::interpreter::InterpreterUtils::value_signed_t;
};

// Tests for utility functions

TEST_F(InterpreterTest, MaskValue) {
  value_t value = 0x0123456789ABCDEF;

  value = InterpreterUtils::MaskValue(value, 8);
  EXPECT_EQ(0x0123456789ABCDEF, value);

  value = InterpreterUtils::MaskValue(value, 4);
  EXPECT_EQ(0x0000000089ABCDEF, value);

  value = InterpreterUtils::MaskValue(value, 2);
  EXPECT_EQ(0x000000000000CDEF, value);

  value = InterpreterUtils::MaskValue(value, 1);
  EXPECT_EQ(0x00000000000000EF, value);

  value = InterpreterUtils::MaskValue(value, 0);
  EXPECT_EQ(0x0, value);
}

TEST_F(InterpreterTest, ExtendSignedValue) {
  value_signed_t value_signed;

  // check zero
  value_signed = InterpreterUtils::ExtendSignedValue(0x0000000000000000, 1, 8);
  EXPECT_EQ(0, value_signed);

  // check correct sign extension for negative values to 8 Bytes
  value_signed = InterpreterUtils::ExtendSignedValue(0x00000000000000FF, 1, 8);
  EXPECT_EQ(-1, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0x000000000000FFFF, 2, 8);
  EXPECT_EQ(-1, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0x00000000FFFFFFFF, 4, 8);
  EXPECT_EQ(-1, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0xFFFFFFFFFFFFFFFF, 8, 8);
  EXPECT_EQ(-1, value_signed);

  // check correct sign extension for positive values to 8 Bytes
  value_signed = InterpreterUtils::ExtendSignedValue(0xFFFFFFFFFFFFFF12, 1, 8);
  EXPECT_EQ(0x12, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0xFFFFFFFFFFFF0012, 2, 8);
  EXPECT_EQ(0x12, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0xFFFFFFFF00000012, 4, 8);
  EXPECT_EQ(0x12, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0x0000000000000012, 8, 8);
  EXPECT_EQ(0x12, value_signed);

  // check sign extension to smaller sizes
  value_signed = InterpreterUtils::ExtendSignedValue(0x00000000000000FE, 1, 2);
  EXPECT_EQ(0x000000000000FFFE, value_signed);

  value_signed = InterpreterUtils::ExtendSignedValue(0x000000000000FEDC, 2, 4);
  EXPECT_EQ(0x00000000FFFFFEDC, value_signed);
}

// Tests for selected executor functions

TEST_F(InterpreterTest, ExecuteBranch) {
  CodeContext code_context;
  CodeGen cg{code_context};
  QueryInterpreter interpreter{code_context};
  FunctionBuilder func{code_context, "func", cg.Int32Type(), {}};

  // create basic blocks
  llvm::BasicBlock *bb_true =
      llvm::BasicBlock::Create(cg.GetContext(), "true", func.GetFunction());
  llvm::BasicBlock *bb_false =
      llvm::BasicBlock::Create(cg.GetContext(), "false", func.GetFunction());

  // add branch instruction
  llvm::BasicBlock *bb_start = cg->GetInsertBlock();
  auto *cond_branch = cg->CreateCondBr(cg.ConstBool(true), bb_true, bb_false);

  // add a following instruction in each basic block
  cg->SetInsertPoint(bb_true);
  auto *branch = cg->CreateBr(bb_false);

  cg->SetInsertPoint(bb_false);
  auto *phi = cg->CreatePHI(cg.Int32Type(), 2);
  phi->addIncoming(cg.Const32(111), bb_true);
  phi->addIncoming(cg.Const32(222), bb_start);
  auto *add = cg->CreateAdd(phi, cg.Const32(333));

  func.ReturnAndFinish(add);

  // prepare function state
  QueryInterpreter::FunctionState state{
      cond_branch, QueryInterpreter::StackFrame{code_context}, 0};

  // verify generated LLVM IR
  PL_ASSERT(code_context.Verify());

  // execute first branch instruction (conditional branch)
  interpreter.ExecuteBranch(state);

  // check if branch was executed correctly
  EXPECT_EQ(branch, state.ip);

  // execute second branch instruction (unconditional branch with Phi's)
  interpreter.ExecuteBranch(state);

  // check if branch was executed correctly
  // ip points to first non-phi instruction
  EXPECT_EQ(add, state.ip);
  // phi has been processed correctly
  EXPECT_EQ(111, state.frame.GetValue(phi));
}

TEST_F(InterpreterTest, ExecuteLoad) {
  CodeContext code_context;
  CodeGen cg{code_context};
  QueryInterpreter interpreter{code_context};
  FunctionBuilder func{code_context, "func", cg.VoidType(), {}};

  // prepare some data to load
  uint8_t data_i8 = 0x01;
  uint16_t data_i16 = 0x0123;
  uint32_t data_i32 = 0x01234567;
  uint64_t data_i64 = 0x0123456789ABCDEF;

  auto *pointer_i8 =
      cg->CreateIntToPtr(cg.Const64(reinterpret_cast<uint64_t>(&data_i8)),
                         llvm::PointerType::get(cg.Int8Type(), 0));
  auto *pointer_i16 =
      cg->CreateIntToPtr(cg.Const64(reinterpret_cast<uint64_t>(&data_i16)),
                         llvm::PointerType::get(cg.Int16Type(), 0));
  auto *pointer_i32 =
      cg->CreateIntToPtr(cg.Const64(reinterpret_cast<uint64_t>(&data_i32)),
                         llvm::PointerType::get(cg.Int32Type(), 0));
  auto *pointer_i64 =
      cg->CreateIntToPtr(cg.Const64(reinterpret_cast<uint64_t>(&data_i64)),
                         llvm::PointerType::get(cg.Int64Type(), 0));

  // add load instructions
  auto *load_i8 = cg->CreateLoad(pointer_i8);
  auto *load_i16 = cg->CreateLoad(pointer_i16);
  auto *load_i32 = cg->CreateLoad(pointer_i32);
  auto *load_i64 = cg->CreateLoad(pointer_i64);

  func.ReturnAndFinish();

  // prepare function state
  QueryInterpreter::FunctionState state{
      load_i8, QueryInterpreter::StackFrame{code_context}, 0};

  // verify generated LLVM IR
  PL_ASSERT(code_context.Verify());

  // execute the loads
  interpreter.ExecuteLoad(state);
  interpreter.ExecuteLoad(state);
  interpreter.ExecuteLoad(state);
  interpreter.ExecuteLoad(state);

  // check if the instructions were executed correctly
  EXPECT_EQ(data_i8,
            InterpreterUtils::MaskValue(state.frame.GetValue(load_i8), 1));
  EXPECT_EQ(data_i16,
            InterpreterUtils::MaskValue(state.frame.GetValue(load_i16), 2));
  EXPECT_EQ(data_i32,
            InterpreterUtils::MaskValue(state.frame.GetValue(load_i32), 4));
  EXPECT_EQ(data_i64,
            InterpreterUtils::MaskValue(state.frame.GetValue(load_i64), 8));
}

}  // namespace test
}  // namespace peloton