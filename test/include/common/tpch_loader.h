//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_loader.h
//
// Identification: test/include/common/tpch_loader.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include <tbb/concurrent_queue.h>
#include <tbb/parallel_for_each.h>

#include "codegen/testing_codegen_util.h"
#include "sql/testing_sql_util.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "type/type_id.h"
#include "storage/tuple.h"
#include "common/harness.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "planner/insert_plan.h"
#include "catalog/catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"

namespace peloton {
namespace test {

class TPCHLoader {
 public:
  using TypeId = ::peloton::type::TypeId;

  TPCHLoader(PelotonCodeGenTest &test_class) : test_class_(test_class),
                                               number_input_tuples_(0),
                                               number_consumed_tuples_(0),
                                               producing_finished_(false),
                                               bulk_size_(100) {
    SetupTableMetadata({"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"});
    number_consumer_threads_ = std::thread::hardware_concurrency();

    insert_queue_.set_capacity(number_consumer_threads_);
  }

  void Load() {
    std::vector<std::thread> producer_threads(number_producer_threads_);
    LOG_INFO("Created %lu producer threads for inserting", number_producer_threads_);
    for (size_t i = 0; i < number_producer_threads_; i++) {
      producer_threads[i] = std::thread([this, i](){
        this->Producer(tables_[i]);
      });
    }

    std::vector<std::thread> consumer_threads(number_consumer_threads_);
    LOG_INFO("Created %lu consumer threads for inserting", number_consumer_threads_);
    for (auto &thread : consumer_threads) {
      thread = std::thread([this](){
        this->Consumer();
      });
    }

    // wait for producer to finish
    for (auto &thread : producer_threads) {
      thread.join();
    }

    // abort queue to terminate finished consumer threads
    LOG_INFO("Sent abort so stop consumer threads");
    producing_finished_ = true;
    insert_queue_.abort();

    // wait for consumer to finish
    for (auto &thread : consumer_threads) {
      thread.join();
    }
  }

 private:
  struct Table {
    std::string name;
    storage::DataTable *data_table;
    std::vector<TypeId> types;
  };

 private:
  void SetupTableMetadata(std::vector<std::string> table_names) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    LOG_INFO("Setting up table meta data");

    for (auto table_name : table_names) {
      storage::DataTable *table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
      std::shared_ptr<catalog::TableCatalogObject> table_object = catalog::Catalog::GetInstance()->GetTableObject(DEFAULT_DB_NAME, table_name, txn);
      std::shared_ptr<catalog::ColumnCatalogObject> column_object;
      std::vector<TypeId> types;

      size_t i = 0;
      while (column_object = table_object->GetColumnObject(i)) {
        types.push_back(column_object->GetColumnType());
        i++;
      }

      std::string in_path = data_path_ + table_name + ".tbl";
      number_input_tuples_ += GetNumberLines(in_path);

      tables_.push_back({table_name, table, types});
    }

    txn_manager.CommitTransaction(txn);

    number_producer_threads_ = table_names.size();
  }

  void Producer(Table &table) {
    std::string in_path = data_path_ + table.name + ".tbl";
    LOG_INFO("Loading from file: %s", in_path.c_str());

    std::ifstream file(in_path);

    if (!file.is_open())
      std::perror("Error opening data file");

    while (file.peek() != EOF) {
      auto plan =
          CreateInsertPlan(file, table.data_table, table.types, bulk_size_);
      //LOG_INFO("Pushed 0x%lX to inserted queue", reinterpret_cast<uintptr_t>(plan.get()));
      insert_queue_.push(plan.release());
    }
  }

  void Consumer() {
    try {
      while (!(producing_finished_ && insert_queue_.size() == 0)) {
        planner::InsertPlan *plan_raw;

        // blocking call
        insert_queue_.pop(plan_raw);
        //LOG_INFO("Popped 0x%lX from inserted queue", reinterpret_cast<uintptr_t>(plan_raw));
        std::unique_ptr<planner::InsertPlan> plan(plan_raw);

        RunInsertPlan(std::move(plan));

        number_consumed_tuples_ += bulk_size_;
        LOG_INFO("%d%%", static_cast<int>(number_consumed_tuples_ * 100 / number_input_tuples_));
      }
    } catch (tbb::user_abort exception) {
      return;
    }
  }

  std::unique_ptr<planner::InsertPlan> CreateInsertPlan(std::ifstream &file, storage::DataTable *table, std::vector<TypeId> types, size_t number_tuples) {
    std::vector<std::vector<ExpressionPtr>> tuples;
    tuples.reserve(number_tuples);

    std::string line;
    size_t i = 0;

    for (; i < number_tuples && std::getline(file, line); i++) {
      tuples.push_back(ParseTuple(line, types));
    }

    std::vector<std::string> columns;
    std::unique_ptr<planner::InsertPlan> plan(
        new planner::InsertPlan(table, &columns, &tuples));

    return plan;
  }

  void RunInsertPlan(std::unique_ptr<planner::InsertPlan> plan) {
    // Bind the plan
    planner::BindingContext context;
    plan->PerformBinding(context);

    // Prepare a consumer to collect the result
    codegen::BufferingConsumer buffer{{0, 1}, context};

    // Compile and execute
    // TODO: make cached
    test_class_.CompileAndExecute(*plan, buffer);
  }

  std::vector<ExpressionPtr> ParseTuple(std::string &line, std::vector<TypeId> types) {
    auto iline = std::stringstream(line);
    std::vector<ExpressionPtr> tuple;
    tuple.reserve(types.size());

    for (auto type : types) {
      std::string cell;
      std::getline(iline, cell, '|');

      type::Value value = ParseValue(cell, type);
      tuple.push_back(ExpressionPtr(new expression::ConstantValueExpression(value)));
    }

    return tuple;
  }

  type::Value ParseValue(std::string &input, TypeId type) {
    switch (type) {
      case TypeId::INTEGER: {
        double value = std::stod(input);
        return type::ValueFactory::GetDecimalValue(value);
      }

      case TypeId::DECIMAL: {
        double value = std::stod(input);
        return type::ValueFactory::GetDecimalValue(value);
      }

      case TypeId::VARCHAR: {
        return type::ValueFactory::GetVarcharValue(input);
      }

      case TypeId::DATE: {
        uint32_t date = ConvertDate(input);
        return type::ValueFactory::GetDateValue(date);
      }

      default:
        throw NotImplementedException("type not supported");
    }
  }

 private:
  // Convert the given string into a i32 date
  uint32_t ConvertDate(std::string &input) {
    std::tm date;
    PELOTON_MEMSET(&date, 0, sizeof(std::tm));
    strptime(input.data(), "%Y-%m-%d", &date);
    date.tm_isdst = -1;
    return static_cast<uint32_t>(mktime(&date));
  }

  size_t GetNumberLines(std::string path) const {
    std::ifstream in(path);

    // new lines will be skipped unless we stop it from happening:
    in.unsetf(std::ios_base::skipws);

    // count the newlines with an algorithm specialized for counting:
    size_t line_count = std::count(
        std::istream_iterator<char>(in),
        std::istream_iterator<char>(),
        '\n');

    return line_count;
  }

  PelotonCodeGenTest &test_class_;
  std::vector<Table> tables_;
  size_t number_producer_threads_;
  size_t number_consumer_threads_;
  size_t number_input_tuples_;
  std::atomic<size_t> number_consumed_tuples_;
  std::atomic<bool> producing_finished_;
  size_t bulk_size_;

  tbb::concurrent_bounded_queue<planner::InsertPlan *> insert_queue_;

  const std::string data_path_ = "/home/marcel/dev/peloton/tpch-dbgen/data/";
};

}  // namespace test
}  // namespace peloton
