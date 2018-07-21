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

import org.apache.drill.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.mapr.tests.annotations.ClusterTest;

@Category(ClusterTest.class)
public class TestComplexTypeFullTableAndIndex extends TestComplexTypeIndex {

  @BeforeClass
  public static void setupTestComplexTypeFullTableAndIndex() throws Exception {
    test(ComplexFTSTypePlanning);
  }

  @AfterClass
  public static void cleanupTestComplexTypeFullTableAndIndex() throws Exception {
    test(ResetComplexFTSTypePlanning);
  }

  @Test
  public void SemiJoinNonCoveringWithRangeCondition_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "SELECT _id from hbase.`index_test_complex1` where _id in "
              + " (select _id from (select _id, flatten(weight) as f from hbase.`index_test_complex1`) as t "
              + " where t.f.low > 120 and t.f.high < 200) ";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*>.*120.*"},
              new String[]{"Join",".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void SemiJoinWithEqualityConditionOnOuterTable_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
              "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t " +
              "where t.f.low <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*<=.*20.*"},
              new String[]{"Join",".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void SemiJoinWithSubqueryConditionOnITEMField_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = " select _id from hbase.`index_test_complex1` t where _id in " +
              "(select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 where t1.`salary`.`max` > 10) as t " +
              "where t.f.high <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*<=.*200.*"},
              new String[]{"Join",".*indexname.*"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void testCoveringIndexWithPredOnIncludedField_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`county` = 'Santa Clara') as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*"},
              new String[]{"Join",".*indexname.*"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void testCoveringIndexWithPredOnITEMIncludedField_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1`" +
              "where _id in (select _id from (select _id from (select _id, county, flatten(weight) as f from hbase.`index_test_complex1` as t1" +
              " where t1.`salary`.`max` > 100) as t where t.f.high > 10))";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*high.*>.*10.*"},
              new String[]{"Join",".*indexname.*"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void SemiJoinWithInnerTableConditionOnArrayAndNonArrayField_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1` t where _id in " +
              "(select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2 " +
              "where t2.f.low <= 200 and t2.minimum_salary >= 0) and t.`county` = 'Santa Clara'";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*"},
              new String[]{".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }

    return;
  }

  @Test
  public void SemiJoinWithStarOnOuterTable_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select * from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f, t1.`salary`.`min` as minimum_salary from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f.low <= 20 and t2.minimum_salary >= 0)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",".*indexname.*"}
      );
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void SemiJoinCoveringIndexPlan_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f.low <= 20 )";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void SemiJoinWithStarAndid_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select * from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f.low <= 200 ) and t.`county` = 'Santa Clara'";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*20.*"},
              new String[]{"Join", "RowKeyJoin",".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  // This test is failing because of a bug in RecordBatchSizer code.
  // TODO Enable this test once the bug is fixed.
  @Ignore
  @Test
  public void SemiJoinWithFlattenOnLeftSide_2_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id, flatten(t.weight) f from hbase.`index_test_complex1` as t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*"},
              new String[]{".*indexname.*"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }

    return;
  }

  /*
   *Index planning should not happen when the tables are different on either side of a IN join.
   * The following test case will not produce any RowKey  join or covering index plan.
   */
  @Test
  public void SemiJoinWithTwoDifferentTables_fts() throws Exception {

    try {
      test(noIndexPlan);
      String query = "select _id, flatten(t.weight) f from hbase.`index_test_complex_without_index` as t " +
              "where _id in (select _id from (select _id, flatten(t1.weight) as f from hbase.`index_test_complex1` as t1 ) as t2 where t2.f.low <= 200)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex_without_index,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex_without_index,",
                      ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*.low.*<=.*200.*",".*indexname.*"}
      );

      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }

    return;
  }

  // Index on scalar array, flatten of scalar array
  @Test
  public void SemiJoinCoveringIndexScalarArray_1_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1` t " +
              "where _id in (select _id from (select _id, flatten(t1.cars) as f from hbase.`index_test_complex1` as t1 ) as t2" +
              " where t2.f like 'Toyota%' )";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*cars.*MATCHES.*Toyota.*"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,.*columns=.*`\\*\\*`.*",".*indexname.*"}
      );
      testBuilder()
              .optionSettingQueriesForBaseline(DisableComplexFTSTypePlanning)
              .optionSettingQueriesForTestQuery(ComplexFTSTypePlanning)
              .unOrdered()
              .sqlQuery(query)
              .sqlBaselineQuery(query)
              .build()
              .run();
    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForMap_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = " select t.salary from hbase.`index_test_complex1` as t " +
              "where t.salary = cast('{\"min\":1000.0, \"max\":2000.0}' as VARBINARY)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[]{".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*salary.*min.*1000.*max.*2000.*"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",".*indexname.*"}
      );

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForList_fts() throws Exception {

    try {
      test(noIndexPlan);
      String query = " select t.weight from hbase.`index_test_complex1` as t " +
              "where t.weight = cast('[{\"low\":120, \"high\":150},{\"low\":110, \"high\":145}]' as VARBINARY)";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*120.*high.*150.*low.*110.*high.*145.*"},
              new String[]{"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",".*indexname.*"}
      );

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForMapWithConjunction_fts() throws Exception {

    try {
      test(noIndexPlan);
      String query = " select t.salary from hbase.`index_test_complex1` as t " +
              "where t.salary = cast('{\"min\":1000.0, \"max\":2000.0}' as VARBINARY) and t.county='Santa Clara'";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*salary.*min.*1000.*max.*2000.*"},
              new String[]{".*indexname.*"}
      );

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestEqualityForListWithConjunction_fts() throws Exception {

    try {
      test(noIndexPlan);
      String query = "select t.weight from hbase.`index_test_complex1` as t " +
              "where t.weight = cast('[{\"low\":120, \"high\":150},{\"low\":110, \"high\":145}]' as VARBINARY) and t.`_id`='user001' ";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*weight.*low.*120.*high.*150.*low.*110.*high.*145.*"},
              new String[]{".*indexname.*"}
      );

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;
  }

  @Test
  public void TestNestedFlattenWithConjunction_fts() throws Exception {
    try {
      test(noIndexPlan);
      String query = "select _id from hbase.`index_test_complex1` as t " +
              " where _id in (select _id from (select _id, flatten(t1.`f1`.`products`) as f2 from (select _id, flatten(orders) as f1 " +
              " from hbase.`index_test_complex1`) as t1) as t2 " +
              " where t2.`f2`.`price` > 50 and t2.`f2`.`prodname` like '%bike%')";

      PlanTestBase.testPlanMatchingPatterns(query,
              new String[] {".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*condition=.*elementAnd.*orders\\[\\]\\.products\\[\\].*price.*prodname"},
              new String[]{".*indexname.*"}
      );

    } finally {
      test(noIndexPlan);
      test(ComplexFTSTypePlanning);
    }
    return;

  }
}
