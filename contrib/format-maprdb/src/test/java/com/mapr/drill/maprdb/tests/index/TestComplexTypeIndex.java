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

import com.mapr.db.Admin;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.impl.TableDescriptorImpl;
import com.mapr.db.tests.utils.DBTests;
import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import com.mapr.drill.maprdb.tests.json.BaseJsonTest;
import com.mapr.tests.annotations.ClusterTest;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.config.DrillConfig;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;

import java.io.InputStream;
import java.util.Properties;

import static com.mapr.drill.maprdb.tests.MaprDBTestsSuite.INDEX_FLUSH_TIMEOUT;

@Category(ClusterTest.class)
public class TestComplexTypeIndex extends BaseJsonTest {

  private static final String TABLE_NAME = "/tmp/index_test_complex1";
  private static final String TABLE_NAME_1 = "/tmp/index_test_complex_without_index";
  private static final String JSON_FILE_URL = "/com/mapr/drill/json/complex_sample1.json";
  private static final String semiColon = ";";

  private static boolean tableCreated = false;
  private static String tablePath;

  private static final String sliceTargetSmall = "alter session set `planner.slice_target` = 1";
  private static final String sliceTargetDefault = "alter session reset `planner.slice_target`";
  private static final String disableRowKeyJoinConversion = "alter session set planner.`enable_rowkeyjoin_conversion` = false";
  public static final String maxNonCoveringSelectivityThreshold = "alter session set `planner.index.noncovering_selectivity_threshold` = 1.0";
  public static final String resetmaxNonCoveringSelectivityThreshold = "alter session reset `planner.index.noncovering_selectivity_threshold`";
  public static final String noIndexPlan = "alter session set `planner.enable_index_planning` = false";
  public static final String IndexPlanning = "alter session set `planner.enable_index_planning` = true";
  public static final String ComplexFTSTypePlanning = "alter session set `planner.enable_complex_type_fts` = true";
  public static final String DisableComplexFTSTypePlanning = "alter session set `planner.enable_complex_type_fts` = false";
  public static final String ResetComplexFTSTypePlanning = "alter session reset `planner.enable_complex_type_fts`";
  public static final String disableHashAgg = "alter session set `planner.enable_hashagg` = false";
  public static final String disableStreamAgg = "alter session set `planner.enable_streamagg` = false";
  public static final String enableHashAgg = "alter session set `planner.enable_hashagg` = true";
  public static final String enableStreamAgg = "alter session set `planner.enable_streamagg` = true";
  public static final String optionsForBaseLine = noIndexPlan + semiColon + DisableComplexFTSTypePlanning;

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
   *   "zipcodes": [94065, 94070],
   *   "friends": [{"name": ["Sam", "Jack"]}],
   *   "orders": [{"products": [{"prodname": "bike", "price": 200.0}]}]
   * }
   */

  @BeforeClass
  public static void setupTestComplexTypeIndex() throws Exception {
    Properties overrideProps = new Properties();
    overrideProps.setProperty("format-maprdb.json.useNumRegionsForDistribution", "true");
    updateTestCluster(1, DrillConfig.create(overrideProps));
    MaprDBTestsSuite.setupTests();
    MaprDBTestsSuite.createPluginAndGetConf(getDrillbitContext());
    tablePath = createTableAndIndex(TABLE_NAME, true, JSON_FILE_URL);
    createTableAndIndex(TABLE_NAME_1, false, JSON_FILE_URL);
    System.out.println("waiting for indexes to sync....");
    Thread.sleep(INDEX_FLUSH_TIMEOUT);
    test(sliceTargetSmall);
    //This setting is disabled to not produce RowKey join in case of two tables not same.
    //These tests are negative tests which test for not producing RowKey Join.
    test(disableRowKeyJoinConversion);
  }

  private static String createTableAndIndex(String tableName, boolean createIndex, String fileName) throws Exception {
    String tablePath;
    final String[] indexList =
        {   "weightIdx1", "weight[].low, weight[].high, weight[].average", "",
            "weightCountyIdx1", "weight[].high", "county,salary.max",
            "salaryWeightIdx1", "salary.max,weight[].high", "salary.min,weight[].low,weight[].high",
            "carsIdx1", "cars[]", "name",
            "carsWeightIdx1", "cars[]", "weight[].low",
            "salaryIdx", "salary", "",
            "weightListIdx", "weight", "",
            "weightComplexIdx1", "weight[].low", "salary,friends,cars,zipcodes",
            "ordersProductsIdx1", "orders[].products[].price, orders[].products[].prodname", "",
            "carsWeightIdx2", "cars[]",  "weight[].high,weight[].average"
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
                      "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t " +
                      "where t.f.low <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*<=.*200.*indexName=(weightIdx1|weightCountyIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = " select _id from hbase.`index_test_complex1` t where _id in " +
              "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 where t1.`salary`.`max` > 10) as t " +
              "where t.f.high <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*<=.*200.*indexName=(salaryWeightIdx1|weightCountyIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
              "(select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2 " +
              "where t2.f.low <= 200 and t2.minimum_salary >= 0) and t.`county` = 'Santa Clara'";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=(weightIdx1|weightComplexIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select * from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f.low <= 200 and t2.minimum_salary >= 0)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=(weightIdx1|weightComplexIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f.low <= 200 )";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=(weightIdx1|weightComplexIdx1)"},
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
    test(IndexPlanning);
    test(DisableComplexFTSTypePlanning);
    String query = "select * from hbase.`index_test_complex1` t " +
            "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
            " where t2.f.low <= 200 ) and t.`county` = 'Santa Clara'";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
            new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",
                    ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=(weightIdx1|weightComplexIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id, t1.`f`.`low` from (select _id, flatten(t.weight) f from hbase.`index_test_complex1` t) as t1 " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*indexName=(weightIdx1|weightComplexIdx1)"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
  @Ignore ("Temporarily ignored")
  @Test
  public void SemiJoinWithTwoDifferentTables() throws Exception {

    try {
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = " select t.weight from hbase.`index_test_complex1` as t " +
              "where t.weight = cast('[{\"average\":135, \"high\":150, \"low\":120},{\"average\":130, \"high\":145, \"low\":110}]' as VARBINARY)";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*average.*135.*high.*150.*low.*120.*average.*130.*high.*145.*low.*110.*indexName=weightListIdx"},
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
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
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select t.weight from hbase.`index_test_complex1` as t " +
          "where t.weight = cast('[{\"average\":135, \"high\":150, \"low\":120},{\"average\":130, \"high\":145, \"low\":110}]' as VARBINARY) and t.`_id`='user001' ";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*average.*135.*high.*150.*low.*120.*average.*130.*high.*145.*low.*110.*indexName=weightListIdx"},
          new String[]{}
          );

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
    }
    return;
  }

  // AND-ed condition on 2 Flattens. Only 1 flatten condition should be pushed down to index; other one
  // should be on primary table as part of non-covering index.
  @Test
  public void TestMultiFlattens_1() throws Exception {

    try {
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select t._id from hbase.`index_test_complex1` as t " +
          " where _id in (select _id from (select _id, flatten(t1.weight) as f1, flatten(t1.weight) as f2 from hbase.`index_test_complex1` as t1) as t2 " +
          "  where t2.f1.low = 150 and t2.f2.high = 165) ";

      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.high.*=.*165",
                  ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*weight.*.low.*=.*150.*|.*weight.*.high.*=.*165.*)indexName=(weightIdx1|weightCountyIdx1)"},
          new String[]{".*condition=.*weight.*.low.*=.*150.*weight.*.high.*=.*165.*indexName=weightIdx1"});

      testBuilder()
        .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
        .optionSettingQueriesForBaseline(optionsForBaseLine)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }


  @Test
  public void testCoveringIndexWithStreamAgg() throws Exception {
    try {
      test(IndexPlanning);
      test(disableHashAgg);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`county` = 'Santa Clara') as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*indexName=weightCountyIdx1"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                            "HashAgg"}
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
      test(enableHashAgg);
    }
    return;
  }

  @Ignore("Ignore until MD-3762 is fixed")
  @Test
  public void testCoveringIndexWithHashAgg() throws Exception {
    try {
      test(IndexPlanning);
      test(disableStreamAgg);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`county` = 'Santa Clara') as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*indexName=weightCountyIdx1"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      "StreamAgg"}
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
      test(enableStreamAgg);
    }
    return;
  }

  @Test
  public void testCoveringIndexWithComplexIncludedField() throws Exception {
    try {
      test(IndexPlanning);
      test(disableHashAgg);
      test(enableStreamAgg);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id, salary, friends, cars, zipcodes from hbase.`index_test_complex1`" +
          " where _id in (select _id from (select _id, flatten(weight) as f from hbase.`index_test_complex1`) as t1" +
          " where t1.f.low < 140)";

      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {".*StreamAgg.*ANY_VALUE-SEMI-JOIN0.*ANY_VALUE-SEMI-JOIN1.*ANY_VALUE-SEMI-JOIN2.*",
              ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*<.*140.*indexName=weightComplexIdx1"},
          new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,"}
      );

      testBuilder()
          .optionSettingQueriesForBaseline(noIndexPlan)
          .unOrdered()
          .sqlQuery(query)
          .sqlBaselineQuery(query)
          .build()
          .run();
    } finally {
      test(enableHashAgg);
      test(IndexPlanning);
    }
    return;
  }

  @Test
  public void TestElementAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(weight) as f1 from " +
              " hbase.`index_test_complex1`) as t where t.f1.low = 150 and t.f1.high = 180)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*150.*high.*180.*indexName=weightIdx1"},
              new String[]{".*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestMultipleElementAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(weight) as f1 from " +
              " hbase.`index_test_complex1`) as t where t.f1.low = 150 and t.f1.high = 180 and t.f1.`average` = 170)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*150.*high.*180.*average.*170.*indexName=weightIdx1"},
              new String[]{".*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestElementAndWithScalar() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1, t1.`salary`.`max` as maxsal from " +
              " hbase.`index_test_complex1` as t1) as t where  t.f1.low = 150  and maxsal = 5000 and t.f1.average = 170 and t.f1.high = 180 )";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*RestrictedJsonTableGroupScan.*condition=.*elementAnd.*weight.*low.*150.*average.*170.*high.*180",".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*150.*high.*180.*average.*170.*indexName=weightIdx1"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestElementAndWithIn() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1 from " +
              " hbase.`index_test_complex1` as t1) as t where t.f1.low in \n" +
              "          (120,150,170) and t.f1.high in (170,180,190))";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*120.*low.*150.*low.*170.*and.*high.*170.*high.*180.*high.*190.*indexName=weightIdx1"},
              new String[]{".*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestElementAndWithOr() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1, t1.`salary`.`max` as maxsal from " +
              " hbase.`index_test_complex1` as t1) as t where (t.f1.low = 120 or maxsal = 2000) and t.f1.average = 135 and t.f1.high = 150)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*elementAnd.*weight.*average.*135.*high.*150.*weight.*low.*120"},
              new String[]{}
      );
      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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

  // Currently in the index condition both low and high are grouped together which is not expected.
  // Ignore this until that is fixed
  @Ignore
  @Test
  public void TestComplexNestedConditions() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1 , t1.`salary`.`max`" +
              " as maxsal from hbase.`index_test_complex1` as t1 ) as t where t.f1.low = 150 and ((t.f1.high = 150 and t.f1.low = 120 ) or (t.f1.low = 150 and t.f1.high = 180)))";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*150.*low.*120.*weight.*low.*150.*high.*180"},
              new String[]{}
      );
      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestInWithAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from " +
              "( select _id, flatten(t1.cars) as f1, t1.`salary`.`max` as maxsal from " +
              "hbase.`index_test_complex1` as t1 ) as t where t.f1 IN ('Toyota Camry', 'BMW') and t.maxsal > 2000)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*Toyota Camry.*cars.*BMW.*indexName.*carsIdx1"},
              new String[]{"UnionExchange"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestInWithElementAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.cars) as f1 " +
              "from hbase.`index_test_complex1`as t1 ) as t where t.f1 IN ('Toyota Camry', 'BMW') " +
              "and t.f1 IN ('Honda Accord', 'Toyota Camry'))";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*Toyota Camry.*or.*BMW.*and.*Honda Accord.*or.*Toyota Camry.*indexName.*(carsIdx1|carsWeightIdx1|carsWeightIdx2)"},
              new String[]{".*elementAnd"}
      );
      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestOrWithAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, " +
              "flatten(t1.weight) as f1 , t1.`salary`.`max` as maxsal from hbase.`index_test_complex1` as t1 ) as t " +
              "where (t.f1.low = 120 or maxsal >= 2000) and (t.f1.low = 140 or t.f1.high = 150))";

      test(maxNonCoveringSelectivityThreshold);
      test(ComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*or.*salary.*max.*2000.*and.*weight.*low.*140.*weight.*high"},
              new String[]{".*elementAnd"}
      );
      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestSimpleOr() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1 ," +
              " t1.`salary`.`max` as maxsal from hbase.`index_test_complex1` as t1 ) as t where t.f1.high = 150 or maxsal = 2000)";

      test(ComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*150.*or.*salary.*max.*2000"},
              new String[]{}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestAndWithElementAnd() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1, flatten(t1.weight) as f2 from " +
              " hbase.`index_test_complex1` as t1) as t where  t.f1.low = 120  and t.f2.high = 145 and t.f1.high = 150)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*RestrictedJsonTableGroupScan.*condition=.*elementAnd.*weight.*low.*120.*high.*150", ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*weight.*low.*120.*high.*150.*|.*weight.*high.*145).*indexName=(weightIdx|weightCountyIdx1)"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestMultipleFlattens_2() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1, flatten(t1.weight) as f2, flatten(t1.weight) as f3 from " +
              " hbase.`index_test_complex1` as t1) as t where  t.f1.low = 120  and t.f2.high = 145 and t.f3.average = 135)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*weight.*low.*120.*|.*weight.*high.*145.*|.*weight.*average.*135).*indexName=(weightIdx|weightCountyIdx1)"},
              new String[]{".*condition=.*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestAndWithIn() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1 , flatten(weight) as f2 from " +
              " hbase.`index_test_complex1` as t1) as t where t.f1.low in \n" +
              "          (120,150,170) and t.f2.high in (170,180,190))";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*weight.*low.*120.*weight.*low.*150.*weight.*low.*170.*|.*weight.*high.*170.*weight.*high.*180.*weight.*high.*190).*indexName=(weightIdx1|weightCountyIdx1)"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestAndWithFullTableScan() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(t1.weight) as f1 , flatten(weight) as f2 from " +
              " hbase.`index_test_complex1` as t1) as t where t.f1.low = 120  and t.f2.high = 145)";

      test(ComplexFTSTypePlanning);
      test(noIndexPlan);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*weight.*low.*120.*|.*weight.*high.*145)"},
              new String[]{".*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
      test(IndexPlanning);
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void testNestedFlattenCovering_1() throws Exception {
    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from " +
                      " (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 from hbase.`index_test_complex1`) as t1) as t2 " +
                      " where t2.`f2`.`price` > 50)";

      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*orders.*products.*price.*50.*indexName=(ordersProductsIdx1)"},
          new String[]{"RowKeyJoin"}
      );
    } finally {

    }
  }

  @Test
  public void testNestedFlattenNonCovering_1() throws Exception {
    try {
      String query = "select _id, name, county from hbase.`index_test_complex1` where _id in (select _id from " +
                      " (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 from hbase.`index_test_complex1`) as t1) as t2 " +
                      " where t2.`f2`.`price` > 50)";

      test(IndexPlanning);
      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                        ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*orders.*.products.*price.*50.*indexName=(ordersProductsIdx1)"},
          new String[]{}
      );
    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
    }
  }

  @Test
  public void testNestedFlattenWithElementAnd() throws Exception {
    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from " +
              " (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 from hbase.`index_test_complex1`) as t1) as t2 " +
              " where t2.`f2`.`price` > 50 and t2.`f2`.`prodname` like '%bike%')";

      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*orders.*products.*price.*50.*and.*prodname.*bike.*indexName=(ordersProductsIdx1)"},
              new String[]{"RowKeyJoin"}
      );
    } finally {

    }
  }

  @Test
  public void testNestedFlattenElementAndWithNonCovering() throws Exception {
    try {
      String query = "select _id, county from hbase.`index_test_complex1` where _id in (select _id from " +
              " (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 from hbase.`index_test_complex1`) as t1) as t2 " +
              " where t2.`f2`.`price` > 50 and t2.`f2`.`prodname` like '%bike%')";

      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*orders.*products.*price.*50.*and.*prodname.*bike.*indexName=(ordersProductsIdx1)"},
              new String[]{}
      );
    } finally {

    }
  }

  @Test
  public void testNestedFlattenNonCovering_2() throws Exception {
    try {
      String query = "select * from hbase.`index_test_complex1` where _id in (select _id from " +
                      " (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 from hbase.`index_test_complex1`) as t1) as t2 " +
                      " where t2.`f2`.`price` > 50)";

      test(IndexPlanning);
      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                        ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*orders.*.products.*price.*50.*indexName=(ordersProductsIdx1)"},
          new String[]{}
      );
    } finally {
      test(resetmaxNonCoveringSelectivityThreshold);
    }
  }

  @Test
  public void TestBooleanIsTrue() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` as t where t.`online` is true and t.`discount`.`eligible` is true";

      test(ComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*online.*=.*true.*.*discount.*eligible.*=.*true)"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .optionSettingQueriesForBaseline(noIndexPlan)
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestBooleanIsFalse() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` as t where t.`online` is false and t.`discount`.`eligible` is false";

      test(ComplexFTSTypePlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=(.*online.*=.*false.*discount.*eligible.*=.*false)"},
              new String[]{}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .optionSettingQueriesForBaseline(noIndexPlan)
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestElementAndwithNoResults() throws Exception {

    try {
      String query = "select _id, county from hbase.`index_test_complex1` where _id in (select _id from ( select _id, flatten(weight) as f1 from " +
              " hbase.`index_test_complex1`) as t where t.f1.low = 175 and t.f1.high = 180)";

      test(maxNonCoveringSelectivityThreshold);
      test(DisableComplexFTSTypePlanning);
      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*175.*high.*180.*indexName=weightIdx1"},
              new String[]{".*elementAnd"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(maxNonCoveringSelectivityThreshold)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestExpressionSimplificationOnMergeRKJ() throws Exception {

    try {
      String query = "SELECT T2.name, T2.W_O.low FROM ( SELECT name, FLATTEN(weight) W_O FROM hbase.`index_test_complex1`" +
              " WHERE _id IN ( SELECT _id FROM ( SELECT C._id, FLATTEN(weight) W_O, C.`salary.max` as max_salary FROM hbase.`index_test_complex1` C" +
              " WHERE max_salary > 5 AND max_salary < 1000 ) T WHERE T.W_O.low = 200 ) ) T2 WHERE T2.W_O.low = 200";

      test(DisableComplexFTSTypePlanning);
      test(IndexPlanning);
      test(maxNonCoveringSelectivityThreshold);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*=.*200.*"},
              new String[]{".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight0.*low.*=.*200.*"}

              );

    } finally {
      test(ResetComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestCoveringMultiFlattenDifferentArrays() throws Exception {
    try {
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
              " (select _id from (select _id, flatten(t1.cars) as f1, flatten(weight) as f2 from hbase.`index_test_complex1` as t1 ) as t2 " +
              "   where t2.f1 like 'Toyota%' and t2.f2.low = 200) ";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*MATCHES.*Toyota.*weight.*low.*=.*200.*indexName=carsWeightIdx1"},
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
  public void TestElementAndwithIncludedFields() throws Exception {

    try {
      String query = "select _id from hbase.`index_test_complex1` where _id in (select _id from (select _id, flatten(t1.cars) as f1, flatten(weight) as f2" +
              " from hbase.`index_test_complex1` as t1 ) as t2 where t2.f1 like 'Toyota%' and t2.f2.high = 150 and t2.f2.average = 135)";

      test(IndexPlanning);

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*elementAnd.*weight.*high.*150.*average.*135.*cars.*MATCHES.*Toyota.*indexName=carsWeightIdx2"},
              new String[]{"RowKeyJoin"}
      );

      testBuilder()
              .optionSettingQueriesForTestQuery(IndexPlanning)
              .optionSettingQueriesForBaseline(optionsForBaseLine)
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
  public void TestNonCoveringMultiFlattenSameArray() throws Exception {
    try {
      test(IndexPlanning);
      test(DisableComplexFTSTypePlanning);
      test(maxNonCoveringSelectivityThreshold);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
          "(select _id from (select _id, flatten(t1.cars) as f1, flatten(weight) as f2 ,flatten(weight) as f3 from hbase.`index_test_complex1` as t1 ) as t2 " +
          "where t2.f1 like 'Toyota%' and t2.f2.average = 120 and t2.f3.high = 150)";
      PlanTestBase.testPlanMatchingPatterns(query,
          new String[] {"RowKeyJoin",
              ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*MATCHES.*Toyota.*weight.*average.*120.*weight.*high.*150",
              ".*JsonTableGroupScan.*tableName=.*index_test_complex1," +
              "(.*condition=.*weight.*high.*150.*indexName=weightCountyIdx1|.*condition=.*cars.*MATCHES.*Toyota.*indexName=carsIdx1)"},
          new String[]{});
      testBuilder()
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
}
