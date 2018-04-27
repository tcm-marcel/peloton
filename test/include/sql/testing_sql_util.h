//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.h
//
// Identification: test/include/sql/sql_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "common/statement.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}

namespace optimizer {
class AbstractOptimizer;
}

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

class TestingSQLUtil {
 public:
  // Show the content in the specific table in the specific database
  // Note: In order to see the content from the command line, you have to
  // turn-on LOG_TRACE.
  static void ShowTable(std::string database_name, std::string table_name);

  // Execute a SQL query end-to-end
  static ResultType ExecuteSQLQuery(const std::string query,
                                    std::vector<ResultValue> &result,
                                    std::vector<FieldInfo> &tuple_descriptor,
                                    int &rows_affected,
                                    std::string &error_message,
                                    IsolationLevelType isolation_level = IsolationLevelType::SERIALIZABLE);

  static ResultType ExecuteSQLQuery(const std::string query,
                                    std::vector<ResultValue> &result,
                                    std::vector<FieldInfo> &tuple_descriptor,
                                    int &rows_affected,
                                    IsolationLevelType isolation_level = IsolationLevelType::SERIALIZABLE);

  // Execute a SQL query end-to-end with the specific optimizer
  // Note: right now this is not executed in the context of a transaction, we
  // may want to pass a transaction pointer here if that API is exposed after
  // the refactor by Siddharth
  static ResultType ExecuteSQLQueryWithOptimizer(
      std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
      const std::string query, std::vector<ResultValue> &result,
      std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
      std::string &error_message,
      IsolationLevelType isolation_level = IsolationLevelType::SERIALIZABLE);

  // Generate the plan tree for a SQL query with the specific optimizer
  static std::shared_ptr<planner::AbstractPlan> GeneratePlanWithOptimizer(
      std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
      const std::string query, concurrency::TransactionContext *txn);

  // A simpler wrapper around ExecuteSQLQuery
  static ResultType ExecuteSQLQuery(const std::string query,
                                    std::vector<ResultValue> &result,
                                    IsolationLevelType isolation_level = IsolationLevelType::SERIALIZABLE);

  // A another simpler wrapper around ExecuteSQLQuery
  static ResultType ExecuteSQLQuery(const std::string query,
                                    IsolationLevelType isolation_level = IsolationLevelType::SERIALIZABLE);

  // Executes a query and compares the result with the given rows, either
  // ordered or not
  // The result vector has to be specified as follows:
  // {"1|string1", "2|strin2", "3|string3"}
  static void ExecuteSQLQueryAndCheckResult(std::string query,
                                            std::vector<std::string> ref_result,
                                            bool ordered = false);

  // Get the return value of one column as string at a given position
  // NOTE: Result columns across different rows are unfolded into a single
  // vector (vector<ResultType>).
  static std::string GetResultValueAsString(
      const std::vector<ResultValue> &result, size_t index) {
    std::string value(result[index].begin(), result[index].end());
    return value;
  }

  // Create a random number
  static int GetRandomInteger(const int lower_bound, const int upper_bound);
  static void UtilTestTaskCallback(void *arg);

  static tcop::TrafficCop traffic_cop_;
  static std::atomic_int counter_;
  //  inline static void SetTrafficCopCounter() {
  //    counter_.store(1);
  //  }
  static void ContinueAfterComplete();
};
}  // namespace test
}  // namespace peloton
