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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.InputStream;
import java.util.Properties;

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
  private static final String JSON_FILE_URL = "/com/mapr/drill/json/complex_sample1.json";

  private static boolean tableCreated = false;
  private static String tablePath;

  private static final String maxNonCoveringSelectivityThreshold = "alter session set `planner.index.noncovering_selectivity_threshold` = 1.0";


  protected String getTablePath() {
    return tablePath;
  }

  /*
   * Sample document from the table:
   * { "user_id":"user001",
   *   "county": "Santa Clara",
   *   "salary": {"min":1000.0, "max":2000.0},
   *   "weight": [{"low":120, "high":150},{"low":110, "high":145}],
   *   "friends": [{"name": ["Sam", "Jack"]}]
   * }
   */

  @BeforeClass
  public static void setupTestComplexTypeIndex() throws Exception {
    try (Table table = createOrReplaceTable(TABLE_NAME);
        InputStream in = MaprDBTestsSuite.getJsonStream(JSON_FILE_URL);
        DocumentStream stream = Json.newDocumentStream(in)) {
      tableCreated = true;
      tablePath = table.getPath().toUri().getPath();

      System.out.println(String.format("Created table %s", tablePath));

      // create indexes on empty table
      createIndexes(tablePath);

      // set stats update interval
      DBTests.setTableStatsSendInterval(1);

      // insert documents in table with 'user_id' as the row key
      for (Document document : stream) {
        table.insert(document, "user_id");
      }
      table.flush();

      System.out.println("Inserted documents. Waiting for indexes to be updated..");

      // wait for indexes to be updated
      DBTests.waitForIndexFlush(table.getPath(), INDEX_FLUSH_TIMEOUT);

      System.out.println("Finished waiting for index updates.");
    }
    System.out.println("Sleep 2 secs....");
    Thread.sleep(2000);
  }

  private static Table createOrReplaceTable(String tableName) {
    Admin admin = MaprDBTestsSuite.getAdmin();
    if (admin != null && admin.tableExists(tableName)) {
      admin.deleteTable(tableName);
    }

    TableDescriptor desc = new TableDescriptorImpl(new Path(tableName));

    return admin.createTable(desc);
  }

  private static void createIndexes(String tablePath) throws Exception {
    String createIndex1 = String.format("maprcli table index add -path "
        + tablePath
        + " -index weightIdx1"
        + " -indexedfields weight[].low, weight[].high ");

    System.out.println("Creating index..");
    TestCluster.runCommand(createIndex1);
  }

  @AfterClass
  public static void cleanupTestComplexTypeIndex() throws Exception {
    if (tableCreated) {
      Admin admin = MaprDBTestsSuite.getAdmin();
      if (admin != null && admin.tableExists(TABLE_NAME)) {
        admin.deleteTable(TABLE_NAME);
      }
    }
  }

  @Test
  public void NonCoveringPlan1() throws Exception {

    String query = "SELECT user_id from hbase.`index_test_complex1` where _id in "
        + " (select _id from (select _id, flatten(weight) as f from hbase.`index_test_complex1`) as t "
        + " where t.f.low > 120 and t.f.high < 200) ";

    test(maxNonCoveringSelectivityThreshold);

    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_complex1,",
           ".*JsonTableGroupScan.*tableName=.*index_test_complex1,.*indexName=weightIdx1"},
        new String[]{}
    );

    System.out.println("Non-Covering Plan Verified!");

    return;
  }

}
