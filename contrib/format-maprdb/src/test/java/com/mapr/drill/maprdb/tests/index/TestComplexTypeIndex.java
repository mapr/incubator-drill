/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapr.drill.maprdb.tests.index;

import static com.mapr.drill.maprdb.tests.MaprDBTestsSuite.INDEX_FLUSH_TIMEOUT;


import java.io.InputStream;
import org.apache.drill.PlanTestBase;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;
import com.mapr.db.Admin;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.impl.TableDescriptorImpl;
import com.mapr.db.tests.utils.DBTests;
import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import com.mapr.tests.annotations.ClusterTest;
import com.mapr.drill.maprdb.tests.json.BaseJsonTest;
import com.mapr.fs.utils.ssh.TestCluster;

@Category(ClusterTest.class)
public class TestComplexTypeIndex extends BaseJsonTest {

  private static final String TABLE_NAME = "/tmp/index_test_complex1";
  private static final String TABLE_NAME_1 = "/tmp/index_test_complex_without_index";
  private static final String JSON_FILE_URL = "/com/mapr/drill/json/complex_sample1.json";

  private static boolean tableCreated = false;
  private static String tablePath;

  private static final String maxNonCoveringSelectivityThreshold = "alter session set `planner.index.noncovering_selectivity_threshold` = 1.0";
  private static final String resetmaxNonCoveringSelectivityThreshold = "alter session reset `planner.index.noncovering_selectivity_threshold`";
  private static final String noIndexPlan = "alter session set `planner.enable_index_planning` = false";
  private static final String IndexPlanning = "alter session set `planner.enable_index_planning` = true";


  protected String getTablePath() {
    return tablePath;
  }

  /*
   * Sample document from the table:
   * { "_id":"user001",
   *   "name": "Tom",
   *   "county": "Santa Clara",
   *   "salary": {"min":1000.0, "max":2000.0},
   *   "weight": [{"low":120, "high":150},{"low":110, "high":145}],
   *   "cars": ["Nissan Leaf", "Honda Accord"],
   *   "friends": [{"name": ["Sam", "Jack"]}]
   * }
   */

  @BeforeClass
  public static void setupTestComplexTypeIndex() throws Exception {
    tablePath = createTableAndIndex(TABLE_NAME, true, JSON_FILE_URL);
    createTableAndIndex(TABLE_NAME_1, false, JSON_FILE_URL);
    System.out.println("waiting for indexes to sync....");
    Thread.sleep(INDEX_FLUSH_TIMEOUT);
  }

  private static String createTableAndIndex(String tableName, boolean createIndex, String fileName) throws Exception {
    String tablePath;
    final String[] indexList =
        {   "weightIdx1", "weight[].low, weight[].high", "",
            "weightCountyIdx1", "weight[].high", "county,salary.max",
            "salaryWeightIdx1", "salary.max,weight[].high", "salary.min,weight[].low,weight[].high",
            "carsIdx1", "cars[]", "name",
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
  public static void cleanupTestComplexTypeIndex() throws Exception {
    if (tableCreated) {
      Admin admin = MaprDBTestsSuite.getAdmin();
      if (admin != null) {
        if (admin.tableExists(TABLE_NAME)) {
          admin.deleteTable(TABLE_NAME);
        }
        if (admin.tableExists(TABLE_NAME_1)) {
          admin.deleteTable(TABLE_NAME_1);
        }
      }
    }
  }

  @Test
  public void SemiJoinNonCoveringWithRangeCondition() throws Exception {
    try {
    String query = "SELECT _id from hbase.`index_test_complex1` where _id in "
        + " (select _id from (select _id, flatten(weight) as f from hbase.`index_test_complex1`) as t "
        + " where t.f.low > 120 and t.f.high < 200) ";

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*>.*120.*indexName=weightIdx1"},
            new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
    );
    testBuilder()
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void SemiJoinWithEqualityConditionOnOuterTable() throws Exception {
    try {
    String query = "select _id from hbase.`index_test_complex1` t where _id in " +
                    "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t " +
                    "where t.f.low <= 200) and t.`_id` = 'user001'";

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*<=.*20.*indexName=(weightIdx1|weightCountyIdx1)"},
            new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
    );
    testBuilder()
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void SemiJoinWithSubqueryConditionOnITEMField() throws Exception {
    try {
    String query = " select _id from hbase.`index_test_complex1` t where _id in " +
            "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 where t1.`salary`.`max` > 10) as t " +
            "where t.f.high <= 200)";

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*<=.*200.*indexName=salaryWeightIdx1"},
            new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
    );

    testBuilder()
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void testCoveringIndexWithPredOnIncludedField() throws Exception {
    try {
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`county` = 'Santa Clara') as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*indexName=weightCountyIdx1"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(noIndexPlan)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(IndexPlanning);
    }
    return;
  }

  @Test
  public void testCoveringIndexWithPredOnITEMIncludedField() throws Exception {
    try {
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`salary`.`max` > 100) as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*indexName=weightCountyIdx1"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(noIndexPlan)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(IndexPlanning);
    }
    return;
  }

  @Test
  public void SemiJoinWithInnerTableConditionOnArrayAndNonArrayField() throws Exception {
    try {
    String query = "select _id from hbase.`index_test_complex1` t where _id in " +
            "(select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2 " +
            "where t2.f.low <= 200 and t2.minimum_salary >= 0) and t.`county` = 'Santa Clara'";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                    ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*indexName=weightIdx1"},
            new String[]{}
    );
    testBuilder()
            .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(resetmaxNonCoveringSelectivityThreshold);
    test(IndexPlanning);
  }

    return;
  }

  @Test
  public void SemiJoinWithStarOnOuterTable() throws Exception {
    try {
    String query = "select * from hbase.`index_test_complex1` t " +
            "where _id in (select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2" +
            " where t2.f.low <= 20 and t2.minimum_salary >= 0) and t.`_id` = 'user001'";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",
                    ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*indexName=weightIdx1"},
            new String[]{}
    );
  } finally {
    test(resetmaxNonCoveringSelectivityThreshold);
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void SemiJoinCoveringIndexPlan() throws Exception {
    try {
    String query = "select _id from hbase.`index_test_complex1` t " +
            "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
            " where t2.f.low <= 20 )";

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*indexName=weightIdx1"},
            new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*"}
    );
    testBuilder()
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void SemiJoinWithStarAndid() throws Exception {
  try {
    String query = "select * from hbase.`index_test_complex1` t " +
            "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
            " where t2.f.low <= 200 ) and t.`county` = 'Santa Clara'";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",
                    ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*indexName=weightIdx1"},
            new String[]{}
    );
    testBuilder()
            .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(resetmaxNonCoveringSelectivityThreshold);
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void SemiJoinWithFlattenOnLeftSide_1() throws Exception {
    try {
    String query = "select _id, t1.`f`.`low` from (select _id, flatten(t.weight) f from hbase.`index_test_complex1` t) as t1 " +
            "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                    ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=weightIdx1"},
            new String[]{}
    );
    testBuilder()
            .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(resetmaxNonCoveringSelectivityThreshold);
    test(IndexPlanning);
  }
    return;
  }

  // This test is failing because of a bug in RecordBatchSizer code.
  // TODO Enable this test once the bug is fixed.
  @Ignore
  @Test
  public void SemiJoinWithFlattenOnLeftSide_2() throws Exception {
    try {
      String query = "select _id, flatten(t.weight) f from hbase.`index_test_complex1` as t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=weightIdx1"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(noIndexPlan)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
    }

    return;
  }

  /*
   *Index planning should not happen when the tables are different on either side of a IN join.
   * The following test case will not produce any RowKey  join or covering index plan.
   */
  @Test
  public void SemiJoinWithTwoDifferentTables() throws Exception {

    try {
      String query = "select _id, flatten(t.weight) f from hbase.`index_test_complex_without_index` as t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

      test(maxNonCoveringSelectivityThreshold);


      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex_without_index,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex_without_index,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=weightIdx1"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(noIndexPlan)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
    }

    return;
  }

  // Index on scalar array, flatten of scalar array
  @Test
  public void SemiJoinCoveringIndexScalarArray_1() throws Exception {
    try {
    String query = "select _id from hbase.`index_test_complex1` t " +
            "where _id in (select _id from (select _id, flatten(t1.cars) as f from hbase.`index_test_complex1` as t1 ) as t2" +
            " where t2.f like 'Toyota%' )";

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*MATCHES.*Toyota.*indexName=carsIdx1"},
            new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*"}
    );
    testBuilder()
            .optionSettingQueriesForBaseline(noIndexPlan)
            .unOrdered()
            .sqlQuery(query)
            .sqlBaselineQuery(query)
            .build()
            .run();
  } finally {
    test(IndexPlanning);
  }
    return;
  }

  @Test
  public void TestEqualityForMap() throws Exception {
    try {
      String query = " select t.salary from hbase.`index_test_complex1` as t " +
              "where t.salary = cast('{\"min\":1000.0, \"max\":2000.0}' as VARBINARY)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*salary.*min.*1000.*max.*2000.*indexName=salaryIdx"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
      );

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForList() throws Exception {

    try {
      String query = " select t.weight from hbase.`index_test_complex1` as t " +
              "where t.weight = cast('[{\"low\":120, \"high\":150},{\"low\":110, \"high\":145}]' as VARBINARY)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*120.*high.*150.*low.*110.*high.*145.*indexName=weightListIdx"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
      );

    } finally {
        test(resetmaxNonCoveringSelectivityThreshold);
        test(IndexPlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForMapWithConjunction() throws Exception {

    try {
      String query = " select t.salary from hbase.`index_test_complex1` as t " +
              "where t.salary = cast('{\"min\":1000.0, \"max\":2000.0}' as VARBINARY) and t.county='Santa Clara'";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*salary.*min.*1000.*max.*2000.*indexName=salaryIdx"},
              new String[]{}
      );

    } finally {
        test(resetmaxNonCoveringSelectivityThreshold);
        test(IndexPlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForListWithConjunction() throws Exception {

    try {
      String query = "select t.weight from hbase.`index_test_complex1` as t " +
              "where t.weight = cast('[{\"low\":120, \"high\":150},{\"low\":110, \"high\":145}]' as VARBINARY) and t.`_id`='user001' ";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*120.*high.*150.*low.*110.*high.*145.*indexName=weightListIdx"},
              new String[]{}
      );

      } finally {
          test(resetmaxNonCoveringSelectivityThreshold);
          test(IndexPlanning);
      }
    return;
    }

}
