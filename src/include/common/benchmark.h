//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark.h
//
// Identification: src/include/common/benchmark.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#define BENCHMARK_LEVEL 0
#define BENCHMARK_PCM false
#define BENCHMARK_TIMER true

#include <cpucounters.h>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <numeric>
#include <algorithm>

#include "common/macros.h"
#include "common/logger.h"
#include "common/timer.h"


#define BENCHMARK(level, s, a) BENCHMARK_##level(s, a)

#if (BENCHMARK_LEVEL == 0)
#define BENCHMARK_0(s, a) Benchmark::Get(s, a)
#else
#define BENCHMARK_0(s, a) BenchmarkDummy::instance_
#endif

#if (BENCHMARK_LEVEL == 1)
#define BENCHMARK_1(s, a) Benchmark::Get(s, a)
#else
#define BENCHMARK_1(s, a) BenchmarkDummy::instance_
#endif

#if (BENCHMARK_LEVEL == 2)
#define BENCHMARK_2(s, a) Benchmark::Get(s, a)
#else
#define BENCHMARK_2(s, a) BenchmarkDummy::instance_
#endif

namespace peloton {

class Benchmark {
 public:
  Benchmark() {}

  static ALWAYS_INLINE inline Benchmark &Get(std::string section, std::string activation) {

    auto &b = instances_[section + activation];

    b.section_ = section;
    b.activation_ = activation;

    return b;
  }

  ALWAYS_INLINE inline void Start() {
    if (!active_) return;

    if (use_timer_) {
      timer_.Reset();
      timer_.Start();
    }

    if (use_pcm_)
      states_.first = getSystemCounterState();
  }

  ALWAYS_INLINE inline void Stop() {
    if (!active_) return;

    if (use_timer_) {
      timer_.Stop();
      parameters_["Duration"].push_back(timer_.GetDuration());
    }

    if (use_pcm_) {
      states_.second = getSystemCounterState();
      parameters_["InstructionsPerClock"].push_back(getIPC(states_.first, states_.second));
      parameters_["L2CacheHitRatio"].push_back(getL2CacheHitRatio(states_.first, states_.second));
      parameters_["L3CacheHitRatio"].push_back(getL3CacheHitRatio(states_.first, states_.second));
    }

    Dump();
  }

  void Dump() const {
    std::cout << ">> " << section_ << " >> " << activation_ << std::endl;

    for (auto pair : parameters_) {
      auto stats = VectorStats(pair.second);
      std::cout << "  " << pair.first << ": n=" << pair.second.size() << " σ=" << stats.second << " μ=" << stats.first << std::endl;
    }
  }

  void Reset() {
    parameters_.clear();
  }

  static void DumpAll() {
    for (auto &b : instances_)
      b.second.Dump();
  }

  static void ResetAll() {
    LOG_INFO("Benchmark: reset %lu instances", instances_.size());
    for (auto &b : instances_)
      b.second.Reset();
  }

  static std::pair<double, double> VectorStats(std::vector<double> &v) {
    double mean = std::accumulate(v.begin(), v.end(), 0.0) / v.size();

    std::vector<double> diff(v.size());
    std::transform(v.begin(), v.end(), diff.begin(), [mean](double x) { return x - mean; });

    double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    double stdev = std::sqrt(sq_sum / v.size());

    return std::make_pair(mean, stdev);
  }

  // temporary data
  std::pair<SystemCounterState, SystemCounterState> states_;
  Timer<std::ratio<1, 1000>> timer_;

  // instance data
  std::string section_;
  std::string activation_;
  std::unordered_map<std::string, std::vector<double>> parameters_;

  static std::unordered_map<std::string, Benchmark> instances_;

  // configuration
  static const bool use_timer_ = BENCHMARK_TIMER;
  static const bool use_pcm_= BENCHMARK_PCM;

 public:
  // peloton execution method
  enum class ExecutionMethod {
    Adaptive,
    PlanInterpreter,
    LLVMNative,
    LLVMInterpreter
  };

  static ExecutionMethod execution_method_;
  static bool active_;

 private:
  DISALLOW_COPY(Benchmark)
};

class BenchmarkDummy {
 public:
  BenchmarkDummy() {}
  ALWAYS_INLINE inline void Start() {}
  ALWAYS_INLINE inline void Stop() {}

  static BenchmarkDummy instance_;
};

#if BENCHMARK_PCM
class PCMInit {
 public:
  PCMInit() {
    if (BENCHMARK_PCM) {
      PCM *m = PCM::getInstance();
      m->resetPMU();
      PCM::ErrorCode returnResult = m->program();

      if (returnResult != PCM::Success) {
        LOG_ERROR("Intel's PCM couldn't start");
        LOG_ERROR("Error code: %d", returnResult);
        exit(1);
      } else {
        LOG_INFO("Intel's PCM setup done");
      }
    }
  }

  // pcm initialization
  static PCMInit pcm_init_;
};
#endif

}  // namespace peloton