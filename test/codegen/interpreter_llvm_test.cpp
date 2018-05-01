//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_tpc-h_test.cpp
//
// Identification: test/codegen/interpreter_llvm_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===/

#include "common/benchmark.h"
#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class InterpreterBenchmark : public PelotonCodeGenTest {
 public:
  InterpreterBenchmark() {
#ifndef NDEBUG
    LOG_INFO("Benchmark executed in DEBUG mode!");
#endif
  }
};

TEST_F(InterpreterBenchmark, ) {

}


}  // namespace test
}  // namespace peloton