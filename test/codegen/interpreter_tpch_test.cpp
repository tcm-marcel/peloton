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

namespace peloton {
namespace test {

class InterpreterBenchmark : public PelotonCodeGenTest {
 public:
  InterpreterBenchmark() {
#ifndef NDEBUG
    LOG_INFO("Benchmark executed in DEBUG mode!");
#endif
  }

 public:
  oid_t TestTableId() { return test_table_oids[0]; }

  void CreateTables() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
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

  void LoadData() {

  }

  void DropTables() {
    // free the database just created
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }
};

TEST_F(InterpreterBenchmark, CreateTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  CreateTables();
}

TEST_F(InterpreterBenchmark, LoadData) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  LoadData();
}


TEST_F(InterpreterBenchmark, Q1) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::LLVMInterpreter;

  auto b_execution = BENCHMARK(0, "complete execution", "Q1");

  b_execution.Start();

  auto result = TestingSQLUtil::ExecuteSQLQuery(
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
      "l_linestatus; "
      //"order by "
      //"l_returnflag, "
      //"l_linestatus; "
      );
  ASSERT_EQ(result, ResultType::SUCCESS);

  b_execution.Stop();

  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
}

TEST_F(InterpreterBenchmark, DropTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  DropTables();
}

}  // namespace test
}  // namespace peloton