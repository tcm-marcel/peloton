//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"
#include "storage/storage_manager.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

TEST_F(CatalogTests, BootstrappingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  EXPECT_EQ(1, storage::StorageManager::GetInstance()->GetDatabaseCount());
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  storage::Database *database =
      catalog->GetDatabaseWithName(txn, CATALOG_DATABASE_NAME);
  // Check database metric table
  storage::DataTable *db_metric_table =
      catalog->GetTableWithName(txn,
                                CATALOG_DATABASE_NAME,
                                CATALOG_SCHEMA_NAME,
                                DATABASE_METRICS_CATALOG_NAME);
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(nullptr, database);
  EXPECT_NE(nullptr, db_metric_table);
}
//
TEST_F(CatalogTests, CreatingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(txn, "emp_db");
  EXPECT_EQ("emp_db", catalog::Catalog::GetInstance()
      ->GetDatabaseWithName(txn, "emp_db")
      ->GetDBName());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, CreatingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_2(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_3(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateTable(txn,
                                               "emp_db",
                                               DEFAULT_SCHEMA_NAME,
                                               std::move(table_schema),
                                               "emp_table",
                                               false);
  catalog::Catalog::GetInstance()->CreateTable(txn,
                                               "emp_db",
                                               DEFAULT_SCHEMA_NAME,
                                               std::move(table_schema_2),
                                               "department_table",
                                               false);
  catalog::Catalog::GetInstance()->CreateTable(txn,
                                               "emp_db",
                                               DEFAULT_SCHEMA_NAME,
                                               std::move(table_schema_3),
                                               "salary_table",
                                               false);
  // insert random tuple into DATABASE_METRICS_CATALOG and check
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  catalog::DatabaseMetricsCatalog::GetInstance()->InsertDatabaseMetrics(txn,
                                                                        2,
                                                                        3,
                                                                        4,
                                                                        5,
                                                                        pool.get());

  // inset meaningless tuple into QUERY_METRICS_CATALOG and check
  stats::QueryMetric::QueryParamBuf param;
  param.len = 1;
  param.buf = (unsigned char *) pool->Allocate(1);
  *param.buf = 'a';
  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseCatalogEntry(txn, "emp_db");
  catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_object->GetDatabaseOid())
      ->GetQueryMetricsCatalog()
      ->InsertQueryMetrics(txn,
                           "a query",
                           database_object->GetDatabaseOid(),
                           1,
                           param,
                           param,
                           param,
                           1,
                           1,
                           1,
                           1,
                           1,
                           1,
                           1,
                           pool.get());
  auto param1 = catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_object->GetDatabaseOid())
      ->GetQueryMetricsCatalog()
      ->GetParamTypes(txn, "a query");
  EXPECT_EQ(1, param1.len);
  EXPECT_EQ('a', *param1.buf);
  // check colum object
  EXPECT_EQ("name", catalog::Catalog::GetInstance()
      ->GetTableCatalogEntry(txn,
                             "emp_db",
                             DEFAULT_SCHEMA_NAME,
                             "department_table")
      ->GetColumnCatalogEntry(1)
      ->GetColumnName());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, TestingCatalogCache) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto catalog = catalog::Catalog::GetInstance();
  auto
      catalog_db_object =
      catalog->GetDatabaseCatalogEntry(txn, CATALOG_DATABASE_OID);
  auto catalog_table_objects = catalog_db_object->GetTableCatalogEntries();
  EXPECT_NE(0, catalog_table_objects.size());

  auto user_db_object = catalog->GetDatabaseCatalogEntry(txn, "emp_db");
  auto user_database = storage::StorageManager::GetInstance()
      ->GetDatabaseWithOid(user_db_object->GetDatabaseOid());

  // check expected table object is acquired
  for (oid_t table_idx = 0; table_idx < user_database->GetTableCount();
       table_idx++) {
    auto table = user_database->GetTable(table_idx);
    auto user_table_object =
        user_db_object->GetTableCatalogEntry(table->GetOid());
    EXPECT_EQ(user_db_object->GetDatabaseOid(),
              user_table_object->GetDatabaseOid());
  }

  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, TableObject) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn,
                                                                            "emp_db",
                                                                            DEFAULT_SCHEMA_NAME,
                                                                            "department_table");

  auto index_objects = table_object->GetIndexCatalogEntries();
  auto column_objects = table_object->GetColumnCatalogEntries();

  EXPECT_EQ(1, index_objects.size());
  EXPECT_EQ(2, column_objects.size());

  EXPECT_EQ(table_object->GetTableOid(), column_objects[0]->GetTableOid());
  EXPECT_EQ("id", column_objects[0]->GetColumnName());
  EXPECT_EQ(0, column_objects[0]->GetColumnId());
  EXPECT_EQ(0, column_objects[0]->GetColumnOffset());
  EXPECT_EQ(type::TypeId::INTEGER, column_objects[0]->GetColumnType());
  EXPECT_EQ(type::Type::GetTypeSize(type::TypeId::INTEGER),
            column_objects[0]->GetColumnLength());
  EXPECT_TRUE(column_objects[0]->IsInlined());
  EXPECT_TRUE(column_objects[0]->IsPrimary());
  EXPECT_FALSE(column_objects[0]->IsNotNull());

  EXPECT_EQ(table_object->GetTableOid(), column_objects[1]->GetTableOid());
  EXPECT_EQ("name", column_objects[1]->GetColumnName());
  EXPECT_EQ(1, column_objects[1]->GetColumnId());
  EXPECT_EQ(4, column_objects[1]->GetColumnOffset());
  EXPECT_EQ(type::TypeId::VARCHAR, column_objects[1]->GetColumnType());
  EXPECT_EQ(32, column_objects[1]->GetColumnLength());
  EXPECT_TRUE(column_objects[1]->IsInlined());
  EXPECT_FALSE(column_objects[1]->IsPrimary());
  EXPECT_FALSE(column_objects[1]->IsNotNull());

  // update pg_table SET version_oid = 1 where table_name = department_table
  oid_t department_table_oid = table_object->GetTableOid();
  auto pg_table = catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(table_object->GetDatabaseOid())
      ->GetTableCatalog();
  bool update_result = pg_table->UpdateVersionId(txn, department_table_oid, 1);
  // get version id after update, invalidate old cache
  table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn,
                                                                       "emp_db",
                                                                       DEFAULT_SCHEMA_NAME,
                                                                       "department_table");
  uint32_t version_oid = table_object->GetVersionId();
  EXPECT_NE(department_table_oid, INVALID_OID);
  EXPECT_EQ(update_result, true);
  EXPECT_EQ(version_oid, 1);

  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, TestingNamespace) {
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  // create namespaces emp_ns0 and emp_ns1
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery(
      "create database default_database;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("create schema emp_ns0;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("create schema emp_ns1;"));

  // create emp_table0 and emp_table1 in namespaces
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns0.emp_table0 (a int, b varchar);"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns0.emp_table1 (a int, b varchar);"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns1.emp_table0 (a int, b varchar);"));
  EXPECT_EQ(ResultType::FAILURE,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns1.emp_table0 (a int, b varchar);"));

  // insert values into emp_table0
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns0.emp_table0 values (1, 'abc');"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns0.emp_table0 values (2, 'abc');"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns1.emp_table0 values (1, 'abc');"));

  // select values from emp_table0 and emp_table1
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns0.emp_table1;", {});
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns0.emp_table0;", {"1|abc", "2|abc"});
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns1.emp_table0;", {"1|abc"});
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("commit;"));
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::FAILURE, TestingSQLUtil::ExecuteSQLQuery(
      "select * from emp_ns1.emp_table1;"));
  EXPECT_EQ(ResultType::ABORTED, TestingSQLUtil::ExecuteSQLQuery("commit;"));

  // drop namespace emp_ns0 and emp_ns1
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("drop schema emp_ns0;"));
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns1.emp_table0;", {"1|abc"});
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("commit;"));
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::FAILURE,
            TestingSQLUtil::ExecuteSQLQuery("drop schema emp_ns0;"));
  EXPECT_EQ(ResultType::FAILURE, TestingSQLUtil::ExecuteSQLQuery(
      "select * from emp_ns0.emp_table1;"));
  EXPECT_EQ(ResultType::ABORTED, TestingSQLUtil::ExecuteSQLQuery("commit;"));
}

TEST_F(CatalogTests, DroppingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  // NOTE: everytime we create a database, there will be 9 catalog tables
  // inside. Additionally, we create 3 tables for the test.
  oid_t expected_table_count = CATALOG_TABLES_COUNT + 3;
  EXPECT_EQ(
      expected_table_count,
      (int) catalog->GetDatabaseCatalogEntry(txn,
                                             "emp_db")->GetTableCatalogEntries().size());
  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseCatalogEntry(txn, "emp_db");
  EXPECT_NE(nullptr, database_object);
  catalog::Catalog::GetInstance()->DropTable(txn,
                                             "emp_db",
                                             DEFAULT_SCHEMA_NAME,
                                             "department_table");

  database_object =
      catalog::Catalog::GetInstance()->GetDatabaseCatalogEntry(txn, "emp_db");
  EXPECT_NE(nullptr, database_object);
  auto department_table_object =
      database_object->GetTableCatalogEntry("department_table",
                                            DEFAULT_SCHEMA_NAME);
  // Decrement expected_table_count to account for the dropped table.
  expected_table_count--;
  EXPECT_EQ(
      expected_table_count,
      (int) catalog->GetDatabaseCatalogEntry(txn,
                                             "emp_db")->GetTableCatalogEntries().size());
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(nullptr, department_table_object);

  // Try to drop again
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->DropTable(txn,
                                                          "emp_db",
                                                          DEFAULT_SCHEMA_NAME,
                                                          "department_table"),
               CatalogException);
  EXPECT_EQ(
      expected_table_count,
      (int) catalog->GetDatabaseCatalogEntry(txn,
                                             "emp_db")->GetTableCatalogEntries().size());
  txn_manager.CommitTransaction(txn);

  // Drop a table that does not exist
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->DropTable(txn,
                                                          "emp_db",
                                                          DEFAULT_SCHEMA_NAME,
                                                          "void_table"),
               CatalogException);
  EXPECT_EQ(
      expected_table_count,
      (int) catalog->GetDatabaseCatalogEntry(txn,
                                             "emp_db")->GetTableCatalogEntries().size());
  txn_manager.CommitTransaction(txn);

  // Drop the other table
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable(txn,
                                             "emp_db",
                                             DEFAULT_SCHEMA_NAME,
                                             "emp_table");
  // Account for the dropped table.
  expected_table_count--;
  EXPECT_EQ(
      expected_table_count,
      (int) catalog->GetDatabaseCatalogEntry(txn,
                                             "emp_db")->GetTableCatalogEntries().size());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, "emp_db");

  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseWithName(txn, "emp_db"),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_NE(nullptr, catalog);
}

TEST_F(CatalogTests, LayoutCatalogTest) {
  // This test creates a table, changes its layout.
  // Create another additional layout.
  // Ensure that default is not changed.
  // Drops layout and verifies that the default_layout is reset.
  // It also queries pg_layout to ensure that the entry is removed.

  auto db_name = "temp_db";
  auto table_name = "temp_table";
  auto catalog = catalog::Catalog::GetInstance();

  // Create database.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_EQ(ResultType::SUCCESS, catalog->CreateDatabase(txn, db_name));

  // Create table.
  auto val0 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val0", true);
  auto val1 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val1", true);
  auto val2 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val2", true);
  auto val3 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val3", true);
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({val0, val1, val2, val3}));
  EXPECT_EQ(ResultType::SUCCESS,
            catalog->CreateTable(txn,
                                 db_name,
                                 DEFAULT_SCHEMA_NAME,
                                 std::move(table_schema),
                                 table_name,
                                 false));
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  auto database_oid =
      catalog->GetDatabaseCatalogEntry(txn, db_name)->GetDatabaseOid();
  auto table_object =
      catalog->GetTableCatalogEntry(txn,
                                    db_name,
                                    DEFAULT_SCHEMA_NAME,
                                    table_name);
  auto table_oid = table_object->GetTableOid();
  auto table =
      catalog->GetTableWithName(txn, db_name, DEFAULT_SCHEMA_NAME, table_name);
  auto pg_layout = catalog->GetSystemCatalogs(database_oid)->GetLayoutCatalog();
  txn_manager.CommitTransaction(txn);

  // Check the first default layout
  auto first_default_layout = table->GetDefaultLayout();
  EXPECT_EQ(ROW_STORE_LAYOUT_OID, first_default_layout->GetOid());
  EXPECT_TRUE(first_default_layout->IsRowStore());
  EXPECT_FALSE(first_default_layout->IsColumnStore());
  EXPECT_FALSE(first_default_layout->IsHybridStore());

  // Check the first default layout in pg_layout and pg_table
  txn = txn_manager.BeginTransaction();
  auto first_layout_oid = first_default_layout->GetOid();
  EXPECT_EQ(
      *(first_default_layout.get()),
      *(pg_layout->GetLayoutWithOid(txn, table_oid, first_layout_oid).get()));
  EXPECT_EQ(first_layout_oid,
            catalog->GetTableCatalogEntry(txn,
                                          database_oid,
                                          table_oid)->GetDefaultLayoutOid());
  txn_manager.CommitTransaction(txn);

  // Change default layout.
  std::map<oid_t, std::pair<oid_t, oid_t>> default_map;
  default_map[0] = std::make_pair(0, 0);
  default_map[1] = std::make_pair(0, 1);
  default_map[2] = std::make_pair(1, 0);
  default_map[3] = std::make_pair(1, 1);

  txn = txn_manager.BeginTransaction();
  auto default_layout =
      catalog->CreateDefaultLayout(txn, database_oid, table_oid, default_map);
  EXPECT_NE(nullptr, default_layout);
  txn_manager.CommitTransaction(txn);

  // Check the changed default layout
  auto default_layout_oid = default_layout->GetOid();
  EXPECT_EQ(default_layout_oid, table->GetDefaultLayout()->GetOid());
  EXPECT_FALSE(default_layout->IsColumnStore());
  EXPECT_FALSE(default_layout->IsRowStore());
  EXPECT_TRUE(default_layout->IsHybridStore());

  // Check the changed default layout in pg_layout and pg_table
  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(
      *(default_layout.get()),
      *(pg_layout->GetLayoutWithOid(txn, table_oid, default_layout_oid).get()));
  EXPECT_EQ(default_layout_oid,
            catalog->GetTableCatalogEntry(txn,
                                          database_oid,
                                          table_oid)->GetDefaultLayoutOid());
  txn_manager.CommitTransaction(txn);

  // Create additional layout.
  std::map<oid_t, std::pair<oid_t, oid_t>> non_default_map;
  non_default_map[0] = std::make_pair(0, 0);
  non_default_map[1] = std::make_pair(0, 1);
  non_default_map[2] = std::make_pair(1, 0);
  non_default_map[3] = std::make_pair(1, 1);

  txn = txn_manager.BeginTransaction();
  auto other_layout =
      catalog->CreateLayout(txn, database_oid, table_oid, non_default_map);
  EXPECT_NE(nullptr, other_layout);
  txn_manager.CommitTransaction(txn);

  // Check the created layout
  EXPECT_FALSE(other_layout->IsColumnStore());
  EXPECT_FALSE(other_layout->IsRowStore());
  EXPECT_TRUE(other_layout->IsHybridStore());

  // Check the created layout in pg_layout
  txn = txn_manager.BeginTransaction();
  auto other_layout_oid = other_layout->GetOid();
  EXPECT_EQ(
      *(other_layout.get()),
      *(pg_layout->GetLayoutWithOid(txn, table_oid, other_layout_oid).get()));

  // Check that the default layout is still the same.
  EXPECT_NE(other_layout, table->GetDefaultLayout());
  EXPECT_NE(other_layout_oid,
            catalog->GetTableCatalogEntry(txn,
                                          database_oid,
                                          table_oid)->GetDefaultLayoutOid());
  txn_manager.CommitTransaction(txn);

  // Drop the default layout.
  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(ResultType::SUCCESS,
            catalog->DropLayout(txn,
                                database_oid,
                                table_oid,
                                default_layout_oid));
  txn_manager.CommitTransaction(txn);

  // Check that default layout is reset and set to row_store.
  EXPECT_NE(default_layout, table->GetDefaultLayout());
  EXPECT_TRUE(table->GetDefaultLayout()->IsRowStore());
  EXPECT_FALSE(table->GetDefaultLayout()->IsColumnStore());
  EXPECT_FALSE(table->GetDefaultLayout()->IsHybridStore());
  EXPECT_EQ(ROW_STORE_LAYOUT_OID, table->GetDefaultLayout()->GetOid());

  // Query pg_layout and pg_table to ensure that the entry is dropped
  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(nullptr,
            pg_layout->GetLayoutWithOid(txn, table_oid, default_layout_oid));
  EXPECT_EQ(ROW_STORE_LAYOUT_OID,
            catalog->GetTableCatalogEntry(txn,
                                          database_oid,
                                          table_oid)->GetDefaultLayoutOid());

  // The additional layout must be present in pg_layout
  EXPECT_EQ(
      *(other_layout.get()),
      *(pg_layout->GetLayoutWithOid(txn, table_oid, other_layout_oid).get()));
  txn_manager.CommitTransaction(txn);

  // Drop database
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(txn, db_name);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
