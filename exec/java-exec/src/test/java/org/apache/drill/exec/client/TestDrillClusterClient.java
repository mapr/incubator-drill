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
package org.apache.drill.exec.client;

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillIOException;
import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.rpc.user.clusterclient.ClusterClientBuilders;
import org.apache.drill.exec.rpc.user.clusterclient.DrillConnection;
import org.apache.drill.exec.rpc.user.clusterclient.DrillSession;
import org.apache.drill.exec.rpc.user.clusterclient.zkbased.ZKBasedConnectionPool;
import org.apache.drill.exec.rpc.user.clusterclient.zkbased.ZKBasedEndpointProvider;
import org.apache.drill.test.QueryBuilder.QuerySummaryFuture;
import org.apache.drill.test.QueryBuilder.SummaryOnlyQueryEventListener;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

// should run this individually before we move to exclusive runs of the tests that start cluster
// We should evaluate to either fix this test to run in parallel with other tests or remove these tests
@Ignore
public class TestDrillClusterClient extends DrillSystemTestBase {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillClusterClient.class);
  final ZookeeperHelper zkHelper = new ZookeeperHelper();

  @Test
  public void zkPool() throws Exception {
    startCluster(1);
    ZKBasedConnectionPool pool = null;
    try {
      pool = ClusterClientBuilders.newZKBasedPool()
          .setZKEndpointProviderBuilder(ZKBasedEndpointProvider.newBuilder()
              .withConnectionString(zkHelper.getConfig().getString(ExecConstants.ZK_CONNECTION))
              .withClusterId(zkHelper.getConfig().getString(ExecConstants.SERVICE_NAME))
              .withZKRoot(zkHelper.getConfig().getString(ExecConstants.ZK_ROOT)))
          .build();
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } finally {
        stopCluster();
      }
    }
  }

  @Test
  public void zkPoolMultiSession() throws Exception {
    startCluster(1);
    ZKBasedConnectionPool pool = null;
    try {
      pool = ClusterClientBuilders.newZKBasedPool()
          .setZKEndpointProviderBuilder(ZKBasedEndpointProvider.newBuilder()
              .withConnectionString(zkHelper.getConfig().getString(ExecConstants.ZK_CONNECTION))
              .withClusterId(zkHelper.getConfig().getString(ExecConstants.SERVICE_NAME))
              .withZKRoot(zkHelper.getConfig().getString(ExecConstants.ZK_ROOT)))
          // .setConnectionProperties(null) username, password
          .build();

      final Random random = new Random();
      final List<DrillSession> sessions = new ArrayList<>();
      try {
        int numSessions = 3;
        final int numOperations = 50;
        for (int i = 0; i < numSessions; i++) {
          sessions.add(pool.newSession(null /* impersonation_target */));
        }

        final List<QuerySummaryFuture> results = new ArrayList<>();
        for (int i = 0; i < numOperations; i++) {
          switch (random.nextInt(4)) {
          case 0 :
          case 1 :
            results.add(runSampleQuery(sessions.get(random.nextInt(numSessions))));
            break;
          case 2 :
            sessions.add(pool.newSession(null /* impersonation_target */));
            numSessions++;
            break;
          case 3:
            if (numSessions - 1 != 0) {
              sessions.remove(random.nextInt(numSessions))
                  .close();
              numSessions--;
            }
            break;
          }
        }

//        for (final QuerySummaryFuture result : results) {
//          System.out.println("Final state: " + result.get().finalState()
//              + " or failed? " + result.get().failed());
//        }

      } finally {
        for (DrillSession session : sessions) {
          session.close();
        }
      }
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } finally {
        stopCluster();
      }
    }
  }

  private static void createSessionAndTryQuery(final DrillConnection connection)
      throws DrillIOException, InterruptedException {
    DrillSession session = null;
    try {
      final Properties sessionProperties = new Properties();

      // impersonation options
      sessionProperties.setProperty(DrillProperties.USER, "root");

      // query optimization options
      sessionProperties.setProperty("exec.query.progress.update", "false");
      sessionProperties.setProperty("exec.udf.use_dynamic", "false");
      sessionProperties.setProperty("exec.query_profile.save", "false");
      sessionProperties.setProperty("planner.use_simple_optimizer", "true");

      session = connection.newSession(sessionProperties);

      // Verify all session options are set as above.
      try {
        assertTrue(verifyQueryProgressOption(session).get().recordCount() == 1);
        assertTrue(verifyDynamicUDFOption(session).get().recordCount() == 1);
        assertTrue(verifyQueryProfileOption(session).get().recordCount() == 1);
        assertTrue(verifySimpleOptimizerOption(session).get().recordCount() == 1);
      } catch (AssertionError e) {
        fail(e.getMessage());
      }

      assertTrue(runSampleQuery(session)
          .get()
          .succeeded());
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  private static QuerySummaryFuture runSampleQuery(final DrillSession session) {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.drillbits",
        new SummaryOnlyQueryEventListener(future));
    return future;
  }

  private static QuerySummaryFuture verifyQueryProgressOption(final DrillSession session) {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.options where name like 'exec.query.progress.update' and optionScope like 'SESSION' and bool_val like 'false'",
        new SummaryOnlyQueryEventListener(future));
    return future;
  }

  private static QuerySummaryFuture verifyDynamicUDFOption(final DrillSession session) {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.options where name like 'exec.udf.use_dynamic' and optionScope like 'SESSION' and bool_val like 'false'",
        new SummaryOnlyQueryEventListener(future));
    return future;
  }

  private static QuerySummaryFuture verifyQueryProfileOption(final DrillSession session) {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.options where name like 'exec.query_profile.save' and optionScope like 'SESSION' and bool_val like 'false'",
        new SummaryOnlyQueryEventListener(future));
    return future;
  }

  private static QuerySummaryFuture verifySimpleOptimizerOption(final DrillSession session) {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.options where name like 'planner.use_simple_optimizer' and optionScope like 'SESSION' and bool_val like 'true'",
        new SummaryOnlyQueryEventListener(future));
    return future;
  }
}
