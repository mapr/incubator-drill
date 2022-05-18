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
package org.apache.drill.hbase;

import org.apache.drill.categories.HbaseStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.store.hbase.config.HBasePersistentStoreProvider;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category({SlowTest.class, HbaseStorageTest.class})
public class TestHBaseTableProvider extends BaseHBaseTest {

  @Parameters
  public static List<Supplier> providers() {
    return Arrays.asList(
        () -> new HBasePersistentStoreProvider(storagePluginConfig.getHBaseConf(), "drill_store"),
        () -> new HBasePersistentStoreProvider(storagePluginConfig.getHBaseConf(), "drill_store", "blob_drill_store")
    );
  }

  @Parameter(0)
  public Supplier<HBasePersistentStoreProvider> providerSupplier;

  private HBasePersistentStoreProvider provider;

  @Before // mask HBase cluster start function
  public void setUpBeforeTestHBaseTableProvider() throws Exception {
    provider = providerSupplier.get();
    provider.start();
  }

  @Test
  public void testTableProvider() throws StoreException {
    LogicalPlanPersistence lp = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(config);
    PersistentStore<String> hbaseStore = provider.getOrCreateStore(
            PersistentStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("hbase").build());
    hbaseStore.put("", "v0");
    hbaseStore.put("k1", "v1");
    hbaseStore.put("k2", "v2");
    hbaseStore.put("k3", "v3");
    hbaseStore.put("k4", "v4");
    hbaseStore.put("k5", "v5");
    hbaseStore.put(".test", "testValue");

    assertEquals("v0", hbaseStore.get(""));
    assertEquals("testValue", hbaseStore.get(".test"));

    assertTrue(hbaseStore.contains(""));
    assertFalse(hbaseStore.contains("unknown_key"));

    assertEquals(7, Lists.newArrayList(hbaseStore.getAll()).size());

    PersistentStore<String> hbaseTestStore = provider.getOrCreateStore(
            PersistentStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("hbase.test").build());
    hbaseTestStore.put("", "v0");
    hbaseTestStore.put("k1", "v1");
    hbaseTestStore.put("k2", "v2");
    hbaseTestStore.put("k3", "v3");
    hbaseTestStore.put("k4", "v4");
    hbaseTestStore.put(".test", "testValue");

    assertEquals("v0", hbaseStore.get(""));
    assertEquals("testValue", hbaseStore.get(".test"));

    assertEquals(6, Lists.newArrayList(hbaseTestStore.getAll()).size());
  }

  @After
  public void tearDownTestHBaseTableProvider() {
    if (provider != null) {
      provider.close();
    }
  }
}
