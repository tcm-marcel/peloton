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

#include "common/macros.h"
#include "common/logger.h"
#include "common/timer.h"


#define BENCHMARK(level, s, a) BENCHMARK_##level(s, a)

#if (BENCHMARK_LEVEL == 0)
#define BENCHMARK_0(s, a) Benchmark(s, a)
#else
#define BENCHMARK_0(s, a) BenchmarkDummy()
#endif

#if (BENCHMARK_LEVEL == 1)
#define BENCHMARK_1(s, a) Benchmark(s, a)
#else
#define BENCHMARK_1(s, a) BenchmarkDummy()
#endif

#if (BENCHMARK_LEVEL == 2)
#define BENCHMARK_2(s, a) Benchmark(s, a)
#else
#define BENCHMARK_2(s, a) BenchmarkDummy()
#endif

namespace peloton {

class Benchmark {
 public:
  Benchmark(std::string section = "", std::string activation = "") : section_(section), activation_(activation)  {}

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
    }

    if (use_pcm_)
      states_.second = getSystemCounterState();

    Dump();
  }

  void Dump() {
    std::cout << ">> " << section_ << " >> " << activation_ << std::endl;

    if (use_timer_) {
      std::cout << "Time in ms: " << timer_.GetDuration() << std::endl;
    }

    if (use_pcm_) {
      std::cout << "Instructions per clock: " << getIPC(states_.first, states_.second) << std::endl;
      std::cout << "L2 Hit Ratio: " << getL2CacheHitRatio(states_.first, states_.second) << std::endl;
      std::cout << "L3 Hit Ratio: " << getL3CacheHitRatio(states_.first, states_.second) << std::endl;
    }
  }

  std::string section_;
  std::string activation_;
  std::pair<SystemCounterState, SystemCounterState> states_;
  Timer<std::ratio<1, 1000>> timer_;

 private:
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
};

class BenchmarkDummy {
 public:
  BenchmarkDummy() {}
  ALWAYS_INLINE inline void Start() {}
  ALWAYS_INLINE inline void Stop() {}
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