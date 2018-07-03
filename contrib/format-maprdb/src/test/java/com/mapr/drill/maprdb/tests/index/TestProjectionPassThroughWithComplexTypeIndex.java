/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapr.drill.maprdb.tests.index;

import com.mapr.db.Admin;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.impl.TableDescriptorImpl;
import com.mapr.db.tests.utils.DBTests;
import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import static com.mapr.drill.maprdb.tests.MaprDBTestsSuite.INDEX_FLUSH_TIMEOUT;
import com.mapr.drill.maprdb.tests.json.BaseJsonTest;
import java.io.InputStream;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.util.EncodedSchemaPathSet;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;


public class TestProjectionPassThroughWithComplexTypeIndex extends BaseJsonTest {

  public static final String noIndexPlan = "alter session set `planner.enable_index_planning` = false";
  public static final String IndexPlanning = "alter session set `planner.enable_index_planning` = true";
  public static final String ComplexFTSTypePlanning = "alter session set `planner.enable_complex_type_fts` = true";
  public static final String DisableComplexFTSTypePlanning = "alter session set `planner.enable_complex_type_fts` = false";
  public static final String ResetComplexFTSTypePlanning = "alter session reset `planner.enable_complex_type_fts`";
  public static final String maxNonCoveringSelectivityThreshold = "alter session set `planner.index.noncovering_selectivity_threshold` = 1.0";

  private static final String TABLE_NAME = "/tmp/index_test_projection";
  private static final String JSON_FILE_URL = "/com/mapr/drill/json/complex_sample1.json";

  private static boolean tableCreated = false;
  private static String tablePath;

  @BeforeClass
  public static void setupTestProjectionPassThroughWithComplexTypeIndex() throws Exception {
    tablePath = createTableAndIndex(TABLE_NAME, true, JSON_FILE_URL);
    System.out.println("waiting for indexes to sync....");
    Thread.sleep(INDEX_FLUSH_TIMEOUT);
  }

  private static String createTableAndIndex(String tableName, boolean createIndex, String fileName) throws Exception {
    String tablePath;
    final String[] indexList =
            {"weightIdx1", "weight[].low, weight[].high, weight[].average", "name,cars",
                    "salaryIdx", "salary", "",
                    "weightListIdx", "weight", ""
            };
    try (Table table = createOrReplaceTable(tableName);
         InputStream in = MaprDBTestsSuite.getJsonStream(fileName);
         DocumentStream stream = Json.newDocumentStream(in)) {
      tableCreated = true;
      tablePath = table.getPath().toUri().getPath();

      System.out.println(String.format("Created table %s", tablePath));

      if (createIndex) {
        // create indexes on empty table
        createIndexes(table, indexList);

        // set stats update interval
        DBTests.setTableStatsSendInterval(1);
      }

      // insert documents in table with 'user_id' as the row key
      for (Document document : stream) {
        table.insert(document, "user_id");
      }
      table.flush();

      System.out.println("Inserted documents. Waiting for indexes to be updated..");

      if (createIndex) {
        // wait for indexes to be updated
        DBTests.waitForIndexFlush(table.getPath(), INDEX_FLUSH_TIMEOUT);
      }

      System.out.println("Finished waiting for index updates.");
    }
    return tablePath;
  }

  private static Table createOrReplaceTable(String tableName) {
    Admin admin = MaprDBTestsSuite.getAdmin();
    if (admin != null && admin.tableExists(tableName)) {
      admin.deleteTable(tableName);
    }

    TableDescriptor desc = new TableDescriptorImpl(new Path(tableName));

    return admin.createTable(desc);
  }

  private static void createIndexes(Table table, String[] indexList) throws Exception {
    LargeTableGen gen = new LargeTableGen(MaprDBTestsSuite.getAdmin());
    System.out.println("Creating indexes..");
    gen.createIndex(table, indexList);
  }

  @AfterClass
  public static void cleanupTestProjectionPassThroughWithComplexTypeIndex() throws Exception {
    if (tableCreated) {
      Admin admin = MaprDBTestsSuite.getAdmin();
      if (admin != null) {
        if (admin.tableExists(TABLE_NAME)) {
          admin.deleteTable(TABLE_NAME);
        }
      }
    }
  }

  @Test
  public void test_encoded_fields_list_equality() throws Exception {
    try {
      test(IndexPlanning);
      final String sql = String.format(
              "SELECT\n"
                      + " t.`%s`, t.`$$document`\n"
                      + " from hbase.`index_test_projection` as t where"
                      + " t.weight = cast('[{\"average\":135, \"high\":150, \"low\":120},"
                      + " {\"average\":130, \"high\":145, \"low\":110}]' as VARBINARY)",
              EncodedSchemaPathSet.encode("weight")[0]
      );

      setColumnWidths(new int[]{40, 80});
      runSQLAndVerifyCount(sql, 3);

      final String[] expectedPlan = {"JsonTableGroupScan" +
              ".*condition=.*weight.*average.*135.*high.*150.*low.*120.*average.*130.*high.*145.*low.*110" +
              ".*indexName=weightListIdx",
              "columns=\\[`weight`, `\\$\\$ENC00O5SWSZ3IOQ`, `\\$\\$document`\\]"};
      final String[] excludedPlan = {};

      PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
    } finally {
      test(IndexPlanning);
    }
  }

  @Test
  public void test_encoded_fields_map_equality() throws Exception {
    try {
      test(IndexPlanning);
      final String sql = String.format(
              "SELECT\n"
                      + " t.`%s`, t.`$$document`\n"
                      + " from hbase.`index_test_projection` as t where"
                      + " t.salary = cast('{\"max\":5000.0, \"min\":1500.0}' as VARBINARY)",
              EncodedSchemaPathSet.encode("salary")[0]
      );

      setColumnWidths(new int[]{40, 80});
      runSQLAndVerifyCount(sql, 2);

      final String[] expectedPlan = {"JsonTableGroupScan" +
              ".*condition=.*salary.*max.*5000.*min.*1500.*indexName=.*salaryIdx",
              "columns=\\[`salary`, `\\$\\$ENC00ONQWYYLSPE`, `\\$\\$document`\\]"};
      final String[] excludedPlan = {};

      PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
    } finally {

    }
  }

  // Ignore until MD-4633 is fixed
  @Ignore
  @Test
  public void test_encoded_fields_with_non_covering_index() throws Exception {
    try {
      test(DisableComplexFTSTypePlanning);
      test(IndexPlanning);
      test(maxNonCoveringSelectivityThreshold);
      final String sql = String.format(
              "SELECT\n"
                      + "  `%s`,t.`$$document`\n"
                      + " FROM\n"
                      + " hbase.`index_test_projection` t\n"
                      + " WHERE _id in\n"
                      + " (select _id from ( select _id, flatten(t1.weight) as f1 from"
                      + " hbase.`index_test_projection` as t1) as t where t.f1.low = 120 and t.f1.high = 190)",
              EncodedSchemaPathSet.encode("_id", "county", "cars")[0]);

      final String[] expectedPlan = {"JsonTableGroupScan" +
              ".*condition.*elementAnd.*low.*120.*high.*190",
              "RestrictedJsonTableGroupScan"+
              ".*columns=\\[`_id`, `\\$\\$ENC00L5UWIADDN52W45DZABRWC4TT`, `\\$\\$document`\\]"};

      final String[] excludedPlan = {};

      PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);

      setColumnWidths(new int[]{40, 80});
      runSQLAndVerifyCount(sql, 2);
    } finally {
      test(ResetComplexFTSTypePlanning);
    }
  }

  @Test
  public void test_encoded_fields_with_covering_index() throws Exception {
    try {
      test(DisableComplexFTSTypePlanning);
      test(IndexPlanning);
      final String sql = String.format(
              "SELECT\n"
                      + " `%s`,t.`$$document`\n"
                      + " FROM\n"
                      + " hbase.`index_test_projection` t\n"
                      + " WHERE _id in\n"
                      + " (select _id from ( select _id, flatten(t1.weight) as f1 from"
                      + " hbase.`index_test_projection` as t1) as t where t.f1.low = 120 and t.f1.high = 150)",
              EncodedSchemaPathSet.encode("_id", "name", "cars")[0]);

      setColumnWidths(new int[]{40, 80});
      runSQLAndVerifyCount(sql, 3);

      final String[] expectedPlan = {"JsonTableGroupScan" +
              ".*condition.*elementAnd.*low.*120.*high.*150.*indexName=.*weightIdx1" +
              ".*columns=\\[`_id`, `\\$\\$ENC00L5UWIADOMFWWKADDMFZHG`, `\\$\\$document`, `_id`, `weight`\\]"};
      final String[] excludedPlan = {"RowKeyJoin"};

      PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
    } finally {
      test(ResetComplexFTSTypePlanning);
    }
  }

  @Test
  public void test_encoded_fields_with_complex_FTS() throws Exception {
    try {
      test(ComplexFTSTypePlanning);
      test(noIndexPlan);
      final String sql = String.format(
              "SELECT\n"
                      + " `%s`,t.`$$document`\n"
                      + " FROM\n"
                      + " hbase.`index_test_projection` t\n"
                      + " WHERE _id in\n"
                      + " (select _id from ( select _id, flatten(t1.weight) as f1 from "
                      + " hbase.`index_test_projection` as t1) as t where t.f1.low = 120 and t.f1.high = 150)",
              EncodedSchemaPathSet.encode("_id", "name", "cars")[0]);

      setColumnWidths(new int[]{40, 80});
      runSQLAndVerifyCount(sql, 3);

      final String[] expectedPlan = {"JsonTableGroupScan" +
              ".*condition.*elementAnd.*low.*120.*high.*150" +
              ".*columns=\\[`_id`, `\\$\\$ENC00L5UWIADOMFWWKADDMFZHG`, `\\$\\$document`, `_id`, `weight`\\]"};
      final String[] excludedPlan = {};

      PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
    } finally {
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
  }

}
