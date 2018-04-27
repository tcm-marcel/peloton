//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark.cpp
//
// Identification: src/common/benchmark.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/benchmark.h"

namespace peloton {

#if BENCHMARK_PCM
Benchmark::PCMInit Benchmark::pcm_init_;
#endif

bool Benchmark::active_ = false;
Benchmark::ExecutionMethod Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;

}  // namespace peloton