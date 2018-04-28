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

  class LoadBar {
   public:
    LoadBar(InterpreterBenchmark &test, std::vector<std::string> files) : i_(0) {
      sum_ = 0;
      for (auto &f : files)
        sum_ += GetNumberLines(test.data_path_ + f + ".tbl");

      LOG_INFO("%lu tuples will be loaded", sum_);
    }

    void DumpPercentage() {
      LOG_INFO("  %lu%% ...", (size_t) (i_ * 100 / sum_));
    }

    size_t GetNumberLines(std::string f) const {
      std::ifstream in(f);

      // new lines will be skipped unless we stop it from happening:
      in.unsetf(std::ios_base::skipws);

      // count the newlines with an algorithm specialized for counting:
      size_t line_count = std::count(
          std::istream_iterator<char>(in),
          std::istream_iterator<char>(),
          '\n');

      return line_count;
    }

    size_t sum_;
    size_t i_;
  };

  void LoadData() {
    auto h = [] (std::vector<uint8_t> c) {
      std::vector<bool> r;
      for (uint8_t i = 1; i <= *c.end(); i++) {
        if (std::find(c.begin(), c.end(), i) != c.end())
          r.push_back(true);
        else
          r.push_back(false);
      }
      return r;
    };

    LoadBar b(*this, {"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"});

    LoadDataFromFile("nation", b, {false, true, false, true});
    LoadDataFromFile("region", b, {false, true, true});
    LoadDataFromFile("part", b, {false, true, true, true, true, false, true, false, true});
    LoadDataFromFile("supplier", b, {false, true, true, false, true, false, true});
    LoadDataFromFile("partsupp", b, {false, false, false, false, true});
    LoadDataFromFile("customer", b, {false, true, true, false, true, false, true, true});
    LoadDataFromFile("orders", b, {false, false, true, false, true, true, true, false, true});
    LoadDataFromFile("lineitem", b, h({9, 10, 14, 15, 16}));
  }

  void DropTables() {
    // free the database just created
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  void DoForAllExecutionMethods(std::string section, std::function<void ()> func) {
    Benchmark::active_ = true;

    Benchmark::execution_method_ = Benchmark::ExecutionMethod::PlanInterpreter;
    auto b1 = BENCHMARK(0, section, "plan interpreter");
    b1.Start();
    func();
    b1.Stop();

    Benchmark::execution_method_ = Benchmark::ExecutionMethod::LLVMNative;
    auto b2 = BENCHMARK(0, section, "llvm native");
    b2.Start();
    func();
    b2.Stop();

    Benchmark::execution_method_ = Benchmark::ExecutionMethod::LLVMInterpreter;
    auto b3 = BENCHMARK(0, section, "llvm interpreter");
    b3.Start();
    func();
    b3.Stop();

    Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
    Benchmark::active_ = false;
  }

  /*
  template<typename type, typename... types>
  void ParseTuple(storage::Tuple &tuple) {
    ParseTuple<types>(tuple);

    auto value = type::ValueFactory::GetIntegerValue();
    tuple.SetValue(sizeof...(types), value);

  };

  template<>
  void ParseTuple(storage::Tuple &tuple) {};

  void InsertTuple(storage::DataTable *table) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    // Start a txn for each insert
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<storage::Tuple> tuple();

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    planner::InsertPlan node(table, std::move(tuple));

    // Insert the desired # of tuples
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      executor::InsertExecutor executor(&node, context.get());
      executor.Execute();
    }

    txn_manager.CommitTransaction(txn);
  }
  */

  void LoadDataFromFile(std::string table, LoadBar &b, std::vector<bool> columns) {
    std::string in_path = data_path_ + table + ".tbl";
    LOG_INFO("Loading from file: %s", in_path.c_str());

    std::ifstream file_in(in_path);
    std::string line;
    uint64_t i = 0;

    if (!file_in.is_open())
      std::perror("Error opening data file");

    while(std::getline(file_in, line)) {
      if (i % 1000 == 0) {
        b.DumpPercentage();
      }
      std::istringstream line_in(line);
      std::string sql = "INSERT INTO " + table + " VALUES (";

      for (unsigned int j = 0; j < columns.size(); j++) {
        std::string cell;
        std::getline(line_in, cell, '|');

        if (columns[j])
          sql += "'" + cell + "'";
        else
          sql += cell;

        if (j < columns.size() - 1)
          sql += ", ";
      }

      sql += ");";

      auto result = TestingSQLUtil::ExecuteSQLQuery(sql);
      ASSERT_EQ(result, ResultType::SUCCESS);

      i++;
      b.i_++;
    }

    if (file_in.bad())
      std::perror("Error reading data file");
  };

  // configuration
  const std::string data_path_ = "/home/marcel/dev/peloton/tpch-dbgen/data/";
};

TEST_F(InterpreterBenchmark, CreateTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  CreateTables();
}

TEST_F(InterpreterBenchmark, DISABLED_LoadData) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  LoadData();
}


TEST_F(InterpreterBenchmark, Q1) {
  DoForAllExecutionMethods("TPC-H Q1", [] () {
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
  });
}


TEST_F(InterpreterBenchmark, Q3) {
  DoForAllExecutionMethods("TPC-H Q3", [] () {
    auto result = TestingSQLUtil::ExecuteSQLQuery(
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
        "o_shippriority;"
//        "order by "
//        "sum(l_extendedprice * (1 - l_discount)) desc, "
//        "o_orderdate; "
    );

    ASSERT_EQ(result, ResultType::SUCCESS);
  });
}

TEST_F(InterpreterBenchmark, Q5) {
  DoForAllExecutionMethods("TPC-H Q5", [] () {
    auto result = TestingSQLUtil::ExecuteSQLQuery(
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
        "n_name; "
//        "order by"
//        "sum(l_extendedprice * (1 - l_discount)) desc;"
    );

    ASSERT_EQ(result, ResultType::SUCCESS);
  });
}

TEST_F(InterpreterBenchmark, DropTables) {
  Benchmark::execution_method_ = Benchmark::ExecutionMethod::Adaptive;
  DropTables();
}

}  // namespace test
}  // namespace peloton