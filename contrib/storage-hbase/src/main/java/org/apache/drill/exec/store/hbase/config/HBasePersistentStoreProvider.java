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
package org.apache.drill.exec.store.hbase.config;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreRegistry;
import org.apache.drill.exec.store.sys.store.provider.BasePersistentStoreProvider;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class HBasePersistentStoreProvider extends BasePersistentStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBasePersistentStoreProvider.class);

  static final byte[] FAMILY = Bytes.toBytes("s");

  static final byte[] QUALIFIER = Bytes.toBytes("d");

  private final TableName hbaseTableName;

  private final TableName hbaseBlobTableName;

  private Configuration hbaseConf;

  private Connection connection;

  private Table hbaseTable;

  private Table hbaseBlobTable;

  public HBasePersistentStoreProvider(PersistentStoreRegistry registry) {
    DrillConfig drillConfig = registry.getConfig();
    final Map<String, Object> config = (Map<String, Object>) drillConfig.getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_CONFIG);
    this.hbaseConf = HBaseConfiguration.create();
    this.hbaseConf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "drill-hbase-persistent-store-client");
    if (config != null) {
      for (Map.Entry<String, Object> entry : config.entrySet()) {
        this.hbaseConf.set(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    this.hbaseTableName = TableName.valueOf(drillConfig.getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE));

    if (drillConfig.hasPath(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_BLOB_TABLE)) {
      String blobTableName = drillConfig.getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_BLOB_TABLE);
      this.hbaseBlobTableName = TableName.valueOf(blobTableName);
    } else {
      this.hbaseBlobTableName = null;
    }
  }

  @VisibleForTesting
  public HBasePersistentStoreProvider(Configuration conf, String storeTableName) {
    this.hbaseConf = conf;
    this.hbaseTableName = TableName.valueOf(storeTableName);
    this.hbaseBlobTableName = null;
  }

  @VisibleForTesting
  public HBasePersistentStoreProvider(Configuration conf, String storeTableName, String blobStoreTableName) {
    this.hbaseConf = conf;
    this.hbaseTableName = TableName.valueOf(storeTableName);
    this.hbaseBlobTableName = TableName.valueOf(blobStoreTableName);
  }


  @Override
  public <V> PersistentStore<V> getOrCreateStore(PersistentStoreConfig<V> config) {
    switch (config.getMode()) {
      case BLOB_PERSISTENT:
        if (Objects.nonNull(hbaseBlobTableName)) {
          return new HBasePersistentStore<>(config, this.hbaseBlobTable);
        }
      case PERSISTENT:
        return new HBasePersistentStore<>(config, this.hbaseTable);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public void start() throws IOException {
    this.connection = ConnectionFactory.createConnection(hbaseConf);

    createTableIfNotExist(hbaseTableName);
    this.hbaseTable = connection.getTable(hbaseTableName);

    if (Objects.nonNull(hbaseBlobTableName)) {
      createTableIfNotExist(hbaseBlobTableName);
      this.hbaseBlobTable = connection.getTable(hbaseBlobTableName);
    }
  }

  private void createTableIfNotExist(TableName table) throws IOException {
    try (Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(table)) {
        HTableDescriptor desc = new HTableDescriptor(table);
        desc.addFamily(new HColumnDescriptor(FAMILY).setMaxVersions(1));
        admin.createTable(desc);
      } else {
        HTableDescriptor desc = admin.getTableDescriptor(table);
        if (!desc.hasFamily(FAMILY)) {
          DrillRuntimeException.create("The HBase table %s specified as persistent store exists but " +
                  "does not contain column family: %s",
              table,
              FAMILY);
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    Stream.of(hbaseTable, hbaseBlobTable)
        .filter(Objects::nonNull)
        .forEach(table -> {
          try {
            table.close();
          } catch (IOException e) {
            logger.warn(String.format("Caught exception while closing HBase table %s.", table.getName()), e);
          }
    });

    if (this.connection != null && !this.connection.isClosed()) {
      try {
        this.connection.close();
      } catch (IOException e) {
        logger.warn("Caught exception while closing HBase connection.", e);
      }
      this.connection = null;
    }
  }

}
