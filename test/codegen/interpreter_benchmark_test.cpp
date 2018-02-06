//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_benchmark.cpp
//
// Identification: test/codegen/interpreter_benchmark.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// use --gtest_also_run_disabled_tests to run

#include <include/storage/storage_manager.h>
#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "executor/executor_context.h"
#include "executor/executors.h"
#include "expression/expression_util.h"
#include "expression/operator_expression.h"

#include "codegen/testing_codegen_util.h"
#include "settings/settings_manager.h"

#include <iostream>
#include <fstream>
#include <include/executor/plan_executor.h>
#include <codegen/testing_codegen_util.h>

namespace peloton {
namespace test {

template <typename type_p>
std::shared_ptr<typename std::remove_pointer<type_p>::type> CreateFakeSharedPtr(
    type_p pointer) {
  // Hacky hack to create a fake shared pointer
  // https://stackoverflow.com/questions/28523035/best-way-to-create-a-fake-smart-pointer-when-you-need-one-but-only-have-a-refere
  return std::shared_ptr<typename std::remove_pointer<type_p>::type>(
      std::shared_ptr<typename std::remove_pointer<type_p>::type>(), pointer);
}

class InterpreterBenchmark : public PelotonCodeGenTest {
 public:
  InterpreterBenchmark() {
#ifndef DEBUG
    LOG_INFO("Benchmark executed in DEBUG mode!");
#endif
  }

  std::string table_name = "crazy_table";

 public:
  oid_t TestTableId() { return test_table_oids[0]; }

  void CreateTable() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto *catalog = catalog::Catalog::GetInstance();

    // Columns and schema
    const bool is_inlined = true;
    std::vector<catalog::Column> cols = {
        {type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
         "COL_A", is_inlined},
        {type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
         "COL_B", is_inlined},
        {type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
         "COL_C", is_inlined}};
    std::unique_ptr<catalog::Schema> schema{new catalog::Schema(cols)};

    // Insert table in catalog
    catalog->CreateTable(test_db_name, table_name, std::move(schema), txn);

    txn_manager.CommitTransaction(txn);
  }

  void AddRows(unsigned long number_rows) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();

    auto &table = *GetDatabase().GetTableWithName(table_name);
    auto *table_schema = table.GetSchema();

    // Insert rows
    for (uint32_t i = 0; i < number_rows; i++) {
      storage::Tuple tuple{table_schema, true};
      for (uint32_t j = 0; j < table_schema->GetColumnCount(); j++) {
        auto col = table_schema->GetColumn(j);
        tuple.SetValue(j, type::ValueFactory::GetIntegerValue(i));
      }

      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer tuple_slot_id =
          table.InsertTuple(&tuple, txn, &index_entry_ptr);
      PL_ASSERT(tuple_slot_id.block != INVALID_OID);
      PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    }

    txn_manager.CommitTransaction(txn);
  }

  void RemoveTable() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto *catalog = catalog::Catalog::GetInstance();

    catalog->DropTable(test_db_name, table_name, txn);

    txn_manager.CommitTransaction(txn);
  }

  CodeGenStats RunCodegenPlanXTimes(planner::AbstractPlan &plan,
                       unsigned int number_runs) {
    // Do binding
    planner::BindingContext context;
    plan.PerformBinding(context);

    CodeGenStats stats;

    for (unsigned int i = 0; i < number_runs; i++) {
      // Printing consumer
      codegen::BufferingConsumer buffer{{0, 1, 2}, context};

      // COMPILE and execute
      CodeGenStats current_stats = CompileAndExecute(plan, buffer);
      stats = stats + current_stats;

      // Check that we got all the results
      //const auto &results = buffer.GetOutputTuples();

      //EXPECT_EQ(number_rows, results.size());
    }

    stats = stats / number_runs;
    return stats;
  }

  double RunExecutorPlanXTimes(planner::AbstractPlan &plan,
                               unsigned int number_runs) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();

    // Timer
    Timer<std::ratio<1, 1000>> timer;
    double overall_stats = 0;

    for (unsigned int i = 0; i < number_runs; i++) {
      const std::vector<int> result_format{0, 0, 0};
      std::vector<ResultValue> result;

      auto on_complete = [&result, this](UNUSED_ATTRIBUTE executor::ExecutionResult p_status,
                                         std::vector<ResultValue> &&values) {
        result = std::move(values);
      };

      std::shared_ptr<planner::AbstractPlan> plan_sp =
          CreateFakeSharedPtr(&plan);

      timer.Start();

      executor::PlanExecutor::ExecutePlan(plan_sp, txn, {}, result_format, on_complete);
      timer.Stop();
      overall_stats += timer.GetDuration();
      timer.Reset();

      //EXPECT_EQ(number_rows, result.size() / 3);
    }

    return overall_stats / number_runs;
  }

  void OutputStats(std::string test_name, std::string mode, unsigned int number_rows,
             unsigned int number_runs, CodeGenStats &stats, std::string info = {}) {
    // Open file for results
    std::ofstream results;
    results.open(test_name + ".csv", std::ios::out | std::ios::app);

    double compile_ms = (stats.runtime_stats.jit_compile_ms != 0) ? stats.runtime_stats.jit_compile_ms : stats.runtime_stats.bytecode_compile_ms;

    LOG_INFO(
        "Stats for %d runs on %d rows (%s): ir_gen: %f setup: "
            "%f compile: %f optimize: %f exe_init: %f exe_plan: %f exe_teardown: "
            "%f",
        number_runs, number_rows, mode.c_str(), stats.compile_stats.ir_gen_ms, stats.compile_stats.setup_ms,
        stats.compile_stats.optimize_ms, compile_ms,
        stats.runtime_stats.init_ms, stats.runtime_stats.plan_ms, stats.runtime_stats.tear_down_ms);

    results << mode << ";" << number_runs << ";" << number_rows << ";"
            << stats.compile_stats.ir_gen_ms << ";" << stats.compile_stats.setup_ms << ";"
            << stats.compile_stats.optimize_ms << ";" << compile_ms
            << ";" << stats.runtime_stats.init_ms << ";" << stats.runtime_stats.plan_ms << ";"
            << stats.runtime_stats.tear_down_ms;

    if (info.size() > 0)
      results << ";" << info;

    results << std::endl;

    // Close file
    results.close();
  }

  void OutputStats(std::string test_name, std::string mode, unsigned int number_rows,
                   unsigned int number_runs, double runtime, std::string info = {}) {
    CodeGenStats stats;
    stats.runtime_stats.plan_ms = runtime;

    OutputStats(test_name, mode, number_rows, number_runs, stats, info);
  }
};

TEST_F(InterpreterBenchmark, DISABLED_SelectStar) {
  //
  // SELECT a, b, c FROM table;
  //

  // Configuration
  unsigned int number_runs = 5;

  // Create Table and insert rows
  CreateTable();

  CodeGenStats stats;

  for (unsigned int number_rows_pow = 1; number_rows_pow < 7;
       number_rows_pow++) {
    unsigned int number_rows = std::pow(10, number_rows_pow);

    // Add rows
    // DEBUG hack!
    if (number_rows_pow == 1)
      AddRows(number_rows);
    else
      AddRows(number_rows - std::pow(10, number_rows_pow - 1));

    // Setup the scan plan node
    planner::SeqScanPlan scan{
        GetDatabase().GetTableWithName(table_name), nullptr, {0, 1, 2}};

    //===----------------------------------------------------------------------===//
    // (1) run with interpreter
    //===----------------------------------------------------------------------===//

    // Force interpreter on
    settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                       true);

    stats = RunCodegenPlanXTimes(scan, number_runs);
    OutputStats(typeid(*this).name(), "interpreter", number_rows, number_runs, stats);

    // Force interpreter off
    settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                       false);

    //===----------------------------------------------------------------------===//
    // (2) run with codegen
    //===----------------------------------------------------------------------===//

    stats = RunCodegenPlanXTimes(scan, number_runs);
    OutputStats(typeid(*this).name(), "codegen", number_rows, number_runs, stats);

    //===----------------------------------------------------------------------===//
    // (1) run with legacy execution engine
    //===----------------------------------------------------------------------===//

    // Force codegen off
    settings::SettingsManager::SetBool(settings::SettingId::codegen, false);

    double runtime = RunExecutorPlanXTimes(scan, number_runs);
    OutputStats(typeid(*this).name(), "legacy", number_rows, number_runs, runtime);

    // Turn codegen on again
    settings::SettingsManager::SetBool(settings::SettingId::codegen, true);
  }

  // Remove created table
  RemoveTable();
}

TEST_F(InterpreterBenchmark, DISABLED_SelectPredicate) {
  //
  // SELECT a, b, c FROM table;
  //

  // Configuration
  unsigned int number_runs = 5;

  // Create Table and insert rows
  CreateTable();

  CodeGenStats stats;

  for (unsigned int number_rows_pow = 1; number_rows_pow < 7;
       number_rows_pow++) {
    unsigned int number_rows = std::pow(10, number_rows_pow);

    // Add rows
    // DEBUG hack!
    if (number_rows_pow == 1)
      AddRows(number_rows);
    else
      AddRows(number_rows - std::pow(10, number_rows_pow - 1));

    for (unsigned int selectivity = 0; selectivity <= 100; selectivity += 20) {
      auto selectivity_string = std::to_string(selectivity) + "%";
      int64_t threshold = number_rows * selectivity / 100;

      // Setup the predicate
      ExpressionPtr b_le =
          CmpLteExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(threshold));

      // Setup the scan plan node
      planner::SeqScanPlan scan{
          GetDatabase().GetTableWithName(table_name), b_le.release(),
          {0, 1, 2}};

      //===----------------------------------------------------------------------===//
      // (1) run with interpreter
      //===----------------------------------------------------------------------===//

      // Force interpreter on
      settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                         true);

      stats = RunCodegenPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "interpreter",
                  number_rows,
                  number_runs,
                  stats, selectivity_string);

      // Force interpreter off
      settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                         false);

      //===----------------------------------------------------------------------===//
      // (2) run with codegen
      //===----------------------------------------------------------------------===//

      stats = RunCodegenPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "codegen",
                  number_rows,
                  number_runs,
                  stats, selectivity_string);

      //===----------------------------------------------------------------------===//
      // (1) run with legacy execution engine
      //===----------------------------------------------------------------------===//

      // Force codegen off
      settings::SettingsManager::SetBool(settings::SettingId::codegen, false);

      double runtime = RunExecutorPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "legacy",
                  number_rows,
                  number_runs,
                  runtime, selectivity_string);

      // Turn codegen on again
      settings::SettingsManager::SetBool(settings::SettingId::codegen, true);
    }
  }

  // Remove created table
  RemoveTable();
}

TEST_F(InterpreterBenchmark, DISABLED_SelectPredicate2) {
  //
  // SELECT a, b, c FROM table;
  //

  // Configuration
  unsigned int number_runs = 5;

  // Create Table and insert rows
  CreateTable();

  CodeGenStats stats;

  for (unsigned int number_rows_pow = 1; number_rows_pow < 7;
       number_rows_pow++) {
    unsigned int number_rows = std::pow(10, number_rows_pow);

    // Add rows
    // DEBUG hack!
    if (number_rows_pow == 1)
      AddRows(number_rows);
    else
      AddRows(number_rows - std::pow(10, number_rows_pow - 1));

    for (unsigned int selectivity = 0; selectivity <= 100; selectivity += 20) {
      auto selectivity_string = std::to_string(selectivity) + "%";
      int64_t threshold = number_rows * selectivity / 100;

      // Setup the predicate
      // ((b <= t && a > 0) || c > t)
      //    sel       true     false
      ExpressionPtr b_le =
          CmpLteExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(threshold));
      ExpressionPtr c_gt =
          CmpGtExpr(ColRefExpr(type::TypeId::INTEGER, 2), ConstIntExpr(threshold));
      ExpressionPtr a_gt =
          CmpGtExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(0));
      ExpressionPtr conj_and = ExpressionPtr{new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, b_le.release(), a_gt.release())};
      ExpressionPtr conj_or = ExpressionPtr{new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, conj_and.release(), c_gt.release())};

      // Setup the scan plan node
      planner::SeqScanPlan scan{
          GetDatabase().GetTableWithName(table_name), conj_or.release(),
          {0, 1, 2}};

      //===----------------------------------------------------------------------===//
      // (1) run with interpreter
      //===----------------------------------------------------------------------===//

      // Force interpreter on
      settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                         true);

      stats = RunCodegenPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "interpreter",
                  number_rows,
                  number_runs,
                  stats, selectivity_string);

      // Force interpreter off
      settings::SettingsManager::SetBool(settings::SettingId::codegen_interpreter,
                                         false);

      //===----------------------------------------------------------------------===//
      // (2) run with codegen
      //===----------------------------------------------------------------------===//

      stats = RunCodegenPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "codegen",
                  number_rows,
                  number_runs,
                  stats, selectivity_string);

      //===----------------------------------------------------------------------===//
      // (1) run with legacy execution engine
      //===----------------------------------------------------------------------===//

      // Force codegen off
      settings::SettingsManager::SetBool(settings::SettingId::codegen, false);

      double runtime = RunExecutorPlanXTimes(scan, number_runs);
      OutputStats(typeid(*this).name(),
                  "legacy",
                  number_rows,
                  number_runs,
                  runtime, selectivity_string);

      // Turn codegen on again
      settings::SettingsManager::SetBool(settings::SettingId::codegen, true);
    }
  }

  // Remove created table
  RemoveTable();
}

}  // namespace test
}  // namespace peloton