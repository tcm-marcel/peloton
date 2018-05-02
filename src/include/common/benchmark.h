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


namespace peloton {

class Benchmark {
 public:
  Benchmark() {}

  static ALWAYS_INLINE inline Benchmark &Get(unsigned int level, std::string section) {

    auto &b = instances_[section];

    b.instance_level_= level;
    b.section_ = section;

    return b;
  }

  static ALWAYS_INLINE inline void Start(unsigned int level, std::string section) {
    if (!active_ || run_level_ != level) return;

    auto &b = Get(level, section);

    if (use_timer_) {
      b.timer_.Reset();
      b.timer_.Start();
    }

    if (use_pcm_)
      b.states_.first = getSystemCounterState();
  }

  static ALWAYS_INLINE inline void Stop(unsigned int level, std::string section) {
    if (!active_ || run_level_ != level) return;

    auto &b = Get(level, section);

    if (use_timer_) {
      b.timer_.Stop();
      b.parameters_["Duration"].push_back(b.timer_.GetDuration());
    }

    if (use_pcm_) {
      b.states_.second = getSystemCounterState();
      b.parameters_["InstructionsPerClock"].push_back(getIPC(b.states_.first, b.states_.second));
      b.parameters_["L2CacheHitRatio"].push_back(getL2CacheHitRatio(b.states_.first, b.states_.second));
      b.parameters_["L3CacheHitRatio"].push_back(getL3CacheHitRatio(b.states_.first, b.states_.second));
    }

    b.Dump();
  }

  void Dump() const {
    std::cout << ">> (" << instance_level_ << ") " << section_ << std::endl;

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

  static void Activate(unsigned int level) {
    if (run_level_ == level)
      active_ = true;
  }

  static void Deactivate(unsigned int level) {
    if (run_level_ == level)
      active_ = false;
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
  unsigned int instance_level_;
  std::unordered_map<std::string, std::vector<double>> parameters_;

 public:
  // peloton execution method
  enum class ExecutionMethod {
    Adaptive,
    PlanInterpreter,
    LLVMNative,
    LLVMInterpreter
  };

  // configuration
  static const bool use_timer_ = BENCHMARK_TIMER;
  static const bool use_pcm_= BENCHMARK_PCM;

  static std::unordered_map<std::string, Benchmark> instances_;
  static ExecutionMethod execution_method_;
  static bool active_;
  static bool run_level_;

 private:
  DISALLOW_COPY(Benchmark)
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