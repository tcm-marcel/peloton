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
PCMInit PCMInit::pcm_init_;
#endif

bool Benchmark::active_ = false;
Benchmark::ExecutionMethod Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
std::unordered_map<std::string, Benchmark> Benchmark::instances_;

BenchmarkDummy BenchmarkDummy::instance_;

}  // namespace peloton