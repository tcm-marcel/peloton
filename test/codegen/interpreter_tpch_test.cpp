//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interpreter_tpc-h_test.cpp
//
// Identification: test/codegen/interpreter_tpc-h_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===/

#include "common/benchmark.h"
#include "codegen/testing_codegen_util.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

#include <iostream>
#include <fstream>
#include <sys/stat.h>

namespace peloton {
namespace test {

class InterpreterBenchmark : public PelotonCodeGenTest {
 public:
  InterpreterBenchmark() {
#ifndef NDEBUG
    LOG_INFO("Benchmark executed in DEBUG mode!");
#endif

    Benchmark::test_case_ = std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
  }

  const size_t runs_ = 10;
  const bool dump_results_ = false;

 public:
  void CreateTables() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
    txn_manager.CommitTransaction(txn);

    ResultType result;

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE nation  ( n_nationkey  INTEGER NOT NULL,"
        "                       n_name       CHAR(25) NOT NULL,"
        "                       n_regionkey  INTEGER NOT NULL,"
        "                       n_comment    VARCHAR(152));");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE region  ( r_regionkey  INTEGER NOT NULL,"
        "                       r_name       CHAR(25) NOT NULL,"
        "                       r_comment    VARCHAR(152));");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE part  ( p_partkey     INTEGER NOT NULL,"
        "                     p_name        VARCHAR(55) NOT NULL,"
        "                     p_mfgr        CHAR(25) NOT NULL,"
        "                     p_brand       CHAR(10) NOT NULL,"
        "                     p_type        VARCHAR(25) NOT NULL,"
        "                     p_size        INTEGER NOT NULL,"
        "                     p_container   CHAR(10) NOT NULL,"
        "                     p_retailprice DECIMAL(15,2) NOT NULL,"
        "                     p_comment     VARCHAR(23) NOT NULL );");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL,"
        "                        s_name        CHAR(25) NOT NULL,"
        "                        s_address     VARCHAR(40) NOT NULL,"
        "                        s_nationkey   INTEGER NOT NULL,"
        "                        s_phone       CHAR(15) NOT NULL,"
        "                        s_acctbal     DECIMAL(15,2) NOT NULL,"
        "                        s_comment     VARCHAR(101) NOT NULL);");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL,"
        "                        ps_suppkey     INTEGER NOT NULL,"
        "                        ps_availqty    INTEGER NOT NULL,"
        "                        ps_supplycost  DECIMAL(15,2)  NOT NULL,"
        "                        ps_comment     VARCHAR(199) NOT NULL );");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE customer ( c_custkey     INTEGER NOT NULL,"
        "                        c_name        VARCHAR(25) NOT NULL,"
        "                        c_address     VARCHAR(40) NOT NULL,"
        "                        c_nationkey   INTEGER NOT NULL,"
        "                        c_phone       CHAR(15) NOT NULL,"
        "                        c_acctbal     DECIMAL(15,2)   NOT NULL,"
        "                        c_mktsegment  CHAR(10) NOT NULL,"
        "                        c_comment     VARCHAR(117) NOT NULL);");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE orders  ( o_orderkey       INTEGER NOT NULL,"
        "                       o_custkey        INTEGER NOT NULL,"
        "                       o_orderstatus    CHAR(1) NOT NULL,"
        "                       o_totalprice     DECIMAL(15,2) NOT NULL,"
        "                       o_orderdate      DATE NOT NULL,"
        "                       o_orderpriority  CHAR(15) NOT NULL,  "
        "                       o_clerk          CHAR(15) NOT NULL, "
        "                       o_shippriority   INTEGER NOT NULL,"
        "                       o_comment        VARCHAR(79) NOT NULL);");
    ASSERT_EQ(result, ResultType::SUCCESS);

    result = TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE lineitem ( l_orderkey    INTEGER NOT NULL,"
        "                        l_partkey     INTEGER NOT NULL,"
        "                        l_suppkey     INTEGER NOT NULL,"
        "                        l_linenumber  INTEGER NOT NULL,"
        "                        l_quantity    DECIMAL(15,2) NOT NULL,"
        "                        l_extendedprice  DECIMAL(15,2) NOT NULL,"
        "                        l_discount    DECIMAL(15,2) NOT NULL,"
        "                        l_tax         DECIMAL(15,2) NOT NULL,"
        "                        l_returnflag  CHAR(1) NOT NULL,"
        "                        l_linestatus  CHAR(1) NOT NULL,"
        "                        l_shipdate    DATE NOT NULL,"
        "                        l_commitdate  DATE NOT NULL,"
        "                        l_receiptdate DATE NOT NULL,"
        "                        l_shipinstruct CHAR(25) NOT NULL,"
        "                        l_shipmode     CHAR(10) NOT NULL,"
        "                        l_comment      VARCHAR(44) NOT NULL);");
    ASSERT_EQ(result, ResultType::SUCCESS);
  }

  void DropTables() {
    // free the database just created
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
    txn_manager.CommitTransaction(txn);
  }

  bool FileExists(const std::string& file) {
    struct stat buf;
    return (stat(file.c_str(), &buf) == 0);
  }

  void DoForAllBenchmarkLevels(std::function<void ()> func) {
    Benchmark::run_level_ = 0;
    func();

    Benchmark::run_level_ = 1;
    func();
  }

  void DoForAllExecutionMethods(size_t times, std::string query, bool dump_results) {
    // Activate benchmarking
    Benchmark::Activate(0);
    Benchmark::Activate(1);

    {
      Benchmark::execution_method_ =
          Benchmark::ExecutionMethod::PlanInterpreter;
      for (size_t i = 0; i < times; i++) {
        Benchmark::Start(0, "plan interpreter");
        auto result = ExecuteQuery(query);
        Benchmark::Stop(0, "plan interpreter");

        std::string filename = Benchmark::test_case_ + "_plan_interpreter.tbl";
        if (dump_results && i == 0 && !FileExists(filename))
          DumpResultToFile(filename, std::move(result));
      }

      Benchmark::ResetAll();
    }

    {
      Benchmark::execution_method_ =
          Benchmark::ExecutionMethod::LLVMNativeNotOptimized;
      for (size_t i = 0; i < times; i++) {
        Benchmark::Start(0, "llvm native");
        auto result = ExecuteQuery(query);
        Benchmark::Stop(0, "llvm native");

        std::string filename = Benchmark::test_case_ + "_llvm_native.tbl";
        if (dump_results && i == 0 && !FileExists(filename))
          DumpResultToFile(filename, std::move(result));
      }

      Benchmark::ResetAll();
    }

    {
      Benchmark::execution_method_ =
          Benchmark::ExecutionMethod::LLVMNativeOptimized;
      for (size_t i = 0; i < times; i++) {
        Benchmark::Start(0, "llvm native-opt");
        auto result = ExecuteQuery(query);
        Benchmark::Stop(0, "llvm native-opt");

        std::string filename = Benchmark::test_case_ + "_llvm_native.tbl";
        if (dump_results && i == 0 && !FileExists(filename))
          DumpResultToFile(filename, std::move(result));
      }

      Benchmark::ResetAll();
    }

    {
      Benchmark::execution_method_ =
          Benchmark::ExecutionMethod::LLVMInterpreterNotOptimized;
      for (size_t i = 0; i < times; i++) {
        Benchmark::Start(0, "llvm interpreter");
        auto result = ExecuteQuery(query);
        Benchmark::Stop(0, "llvm interpreter");

        std::string filename = Benchmark::test_case_ + "_llvm_interpreter.tbl";
        if (dump_results && i == 0 && !FileExists(filename))
          DumpResultToFile(filename, std::move(result));
      }

      Benchmark::ResetAll();
    }

    {
      Benchmark::execution_method_ =
          Benchmark::ExecutionMethod::LLVMInterpreterOptimized;
      for (size_t i = 0; i < times; i++) {
        Benchmark::Start(0, "llvm interpreter-opt");
        auto result = ExecuteQuery(query);
        Benchmark::Stop(0, "llvm interpreter-opt");

        std::string filename = Benchmark::test_case_ + "_llvm_interpreter_opt.tbl";
        if (dump_results && i == 0 && !FileExists(filename))
          DumpResultToFile(filename, std::move(result));
      }

      Benchmark::ResetAll();
    }

    // Reset state
    Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
    Benchmark::Deactivate(1);
    Benchmark::Deactivate(0);
  }

  std::pair<std::vector<ResultValue>, std::vector<FieldInfo>> ExecuteQuery(std::string query) {
    std::vector<ResultValue> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;

    // Execute query
    TestingSQLUtil::ExecuteSQLQuery(std::move(query), result, tuple_descriptor,
                                    rows_changed, error_message);

    size_t number_rows = (tuple_descriptor.size() == 0) ? 0 : result.size() / tuple_descriptor.size();

    LOG_INFO("Query returned %lu rows", number_rows);

    return std::make_pair(std::move(result), std::move(tuple_descriptor));
  }

  void DumpResultToFile(std::string filename, std::pair<std::vector<ResultValue>, std::vector<FieldInfo>> result) {
    std::ofstream file;
    file.open(filename);

    auto &values = result.first;
    auto &tuple_descriptor = result.second;

    unsigned int number_rows = values.size() / tuple_descriptor.size();

    for (unsigned int i = 0; i < number_rows; i++) {
      for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        file << TestingSQLUtil::GetResultValueAsString(
            values, i * tuple_descriptor.size() + j);
        file << "|";
      }
      file << "\n";
    }

    file.close();
  }
};

TEST_F(InterpreterBenchmark, CreateTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  CreateTables();
}

TEST_F(InterpreterBenchmark, LoadData) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;

  std::string path = "/home/marcel/dev/peloton/tpch-dbgen/data/";

  LOG_INFO("Loading table 'nation.tbl'");
  ExecuteQuery("COPY nation FROM '" + path + "nation.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'partsupp.tbl'");
  ExecuteQuery("COPY partsupp FROM '" + path + "partsupp.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'lineitem.tbl'");
  ExecuteQuery("COPY lineitem FROM '" + path + "lineitem.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'orders.tbl'");
  ExecuteQuery("COPY orders FROM '" + path + "orders.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'region.tbl'");
  ExecuteQuery("COPY region FROM '" + path + "region.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'part.tbl'");
  ExecuteQuery("COPY part FROM '" + path + "part.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'supplier.tbl'");
  ExecuteQuery("COPY supplier FROM '" + path + "supplier.tbl' DELIMITER '|'");

  LOG_INFO("Loading table 'customer.tbl'");
  ExecuteQuery("COPY customer FROM '" + path + "customer.tbl' DELIMITER '|'");
}

// TODO:
// cache flushing

// DEBUG
TEST_F(InterpreterBenchmark, DISABLED_DumpTables) {
  for (const std::string &table : {"nation", "region", "part", "supplier",
                                   "partsupp", "customer", "orders",
                                   "lineitem"}) {
    auto result = ExecuteQuery("select * from " + table + ";");
    std::string filename = table + "_dump.tbl";
    if (dump_results_ && !FileExists(filename))
      DumpResultToFile(filename, std::move(result));
  }
}

TEST_F(InterpreterBenchmark, SelectStar) {
  std::string query =
    "select * from customer";

  DoForAllBenchmarkLevels([&]() {
    DoForAllExecutionMethods(runs_, query, dump_results_);
  });
}

TEST_F(InterpreterBenchmark, Q1) {
  std::string query =
    "select "
    "l_returnflag, "
    "l_linestatus, "
    "sum(l_quantity) as sum_qty, "
    "sum(l_extendedprice) as sum_base_price, "
    "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
    "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
    "avg(l_quantity) as avg_qty, "
    "avg(l_extendedprice) as avg_price, "
    "avg(l_discount) as avg_disc, "
    "count(*) as count_order "
    "from "
    "lineitem "
    "where "
    "  l_shipdate <= date '1998-12-01' "
    "group by "
    "l_returnflag, "
    "l_linestatus; ";
    //"order by "
    //"l_returnflag, "
    //"l_linestatus; "

  DoForAllBenchmarkLevels([&]() {
    DoForAllExecutionMethods(runs_, query, dump_results_);
  });
}


TEST_F(InterpreterBenchmark, Q3) {
  std::string query =
    "select "
    "l_orderkey, "
    "sum(l_extendedprice * (1 - l_discount)) as revenue, "
    "o_orderdate, "
    "o_shippriority "
    "from "
    "customer, "
    "orders, "
    "lineitem "
    "where "
    "c_mktsegment = 'MACHINERY' "
    "and c_custkey = o_custkey "
    "and l_orderkey = o_orderkey "
    "and o_orderdate < date '1995-03-10' "
    "and l_shipdate > date '1995-03-10' "
    "group by "
    "l_orderkey, "
    "o_orderdate, "
    "o_shippriority;";
//        "order by "
//        "sum(l_extendedprice * (1 - l_discount)) desc, "
//        "o_orderdate; "

  DoForAllBenchmarkLevels([&]() {
    DoForAllExecutionMethods(runs_, query, dump_results_);
  });
}

TEST_F(InterpreterBenchmark, Q5) {
  std::string query =
    "select "
    "n_name, "
    "sum(l_extendedprice * (1 - l_discount)) as revenue "
    "from "
    "customer, "
    "orders, "
    "lineitem, "
    "supplier, "
    "nation, "
    "region "
    "where "
    "c_custkey = o_custkey "
    "and l_orderkey = o_orderkey "
    "and l_suppkey = s_suppkey "
    "and c_nationkey = s_nationkey "
    "and s_nationkey = n_nationkey "
    "and n_regionkey = r_regionkey "
    "and r_name = 'AFRICA' "
    "and o_orderdate >= date '1997-01-01' "
    "and o_orderdate < date '1998-01-01' "
    "group by "
    "n_name; ";
//        "order by"
//        "sum(l_extendedprice * (1 - l_discount)) desc;"

  DoForAllBenchmarkLevels([&]() {
    DoForAllExecutionMethods(runs_, query, dump_results_);
  });
}

TEST_F(InterpreterBenchmark, Q6) {
  std::string query =
    "select "
    "sum(l_extendedprice * l_discount) as revenue "
    "from "
    "lineitem "
    "where "
    "l_shipdate >= date '1997-01-01' "
    "and l_shipdate < date '1998-01-01' "
    "and l_discount >= (0.07 - 0.01)  "
    "and l_discount <= (0.07 + 0.01) "
    "and l_quantity < 24;";

  DoForAllBenchmarkLevels([&]() {
    DoForAllExecutionMethods(runs_, query, dump_results_);
  });
}

TEST_F(InterpreterBenchmark, DropTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  DropTables();
}

}  // namespace test
}  // namespace peloton