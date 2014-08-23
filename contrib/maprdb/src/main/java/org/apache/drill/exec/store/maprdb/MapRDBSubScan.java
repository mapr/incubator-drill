/**
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
package org.apache.drill.exec.store.maprdb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single HBase region
@JsonTypeName("hbase-region-scan")
public class MapRDBSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBSubScan.class);

  @JsonProperty
  public final HBaseStoragePluginConfig storage;
  @JsonIgnore
  private final HBaseStoragePlugin hbaseStoragePlugin;
  private final List<MapRDBSubScanSpec> regionScanSpecList;
  private final List<SchemaPath> columns;

  @JsonCreator
  public MapRDBSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("storage") StoragePluginConfig storage,
                      @JsonProperty("regionScanSpecList") LinkedList<MapRDBSubScanSpec> regionScanSpecList,
                      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    hbaseStoragePlugin = (HBaseStoragePlugin) registry.getPlugin(storage);
    this.regionScanSpecList = regionScanSpecList;
    this.storage = (HBaseStoragePluginConfig) storage;
    this.columns = columns;
  }

  public MapRDBSubScan(HBaseStoragePlugin plugin, HBaseStoragePluginConfig config,
      List<MapRDBSubScanSpec> regionInfoList, List<SchemaPath> columns) {
    hbaseStoragePlugin = plugin;
    storage = config;
    this.regionScanSpecList = regionInfoList;
    this.columns = columns;
  }

  public List<MapRDBSubScanSpec> getRegionScanSpecList() {
    return regionScanSpecList;
  }

  @JsonIgnore
  public HBaseStoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public HBaseStoragePlugin getStorageEngine(){
    return hbaseStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MapRDBSubScan(hbaseStoragePlugin, storage, regionScanSpecList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class MapRDBSubScanSpec {

    protected String tableName;
    protected String regionServer;
    protected byte[] startRow;
    protected byte[] stopRow;
    protected byte[] serializedFilter;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public MapRDBSubScanSpec(@JsonProperty("tableName") String tableName,
                            @JsonProperty("regionServer") String regionServer,
                            @JsonProperty("startRow") byte[] startRow,
                            @JsonProperty("stopRow") byte[] stopRow,
                            @JsonProperty("serializedFilter") byte[] serializedFilter,
                            @JsonProperty("filterString") String filterString) {
      if (serializedFilter != null && filterString != null) {
        throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
      }
      this.tableName = tableName;
      this.regionServer = regionServer;
      this.startRow = startRow;
      this.stopRow = stopRow;
      if (serializedFilter != null) {
        this.serializedFilter = serializedFilter;
      } else {
        this.serializedFilter = HBaseUtils.serializeFilter(HBaseUtils.parseFilterString(filterString));
      }
    }

    /* package */ MapRDBSubScanSpec() {
      // empty constructor, to be used with builder pattern;
    }

    @JsonIgnore
    private Filter scanFilter;
    public Filter getScanFilter() {
      if (scanFilter == null &&  serializedFilter != null) {
          scanFilter = HBaseUtils.deserializeFilter(serializedFilter);
      }
      return scanFilter;
    }

    public String getTableName() {
      return tableName;
    }

    public MapRDBSubScanSpec setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public String getRegionServer() {
      return regionServer;
    }

    public MapRDBSubScanSpec setRegionServer(String regionServer) {
      this.regionServer = regionServer;
      return this;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public MapRDBSubScanSpec setStartRow(byte[] startRow) {
      this.startRow = startRow;
      return this;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    public MapRDBSubScanSpec setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    public byte[] getSerializedFilter() {
      return serializedFilter;
    }

    public MapRDBSubScanSpec setSerializedFilter(byte[] serializedFilter) {
      this.serializedFilter = serializedFilter;
      this.scanFilter = null;
      return this;
    }

    @Override
    public String toString() {
      return "HBaseScanSpec [tableName=" + tableName
          + ", startRow=" + (startRow == null ? null : Bytes.toStringBinary(startRow))
          + ", stopRow=" + (stopRow == null ? null : Bytes.toStringBinary(stopRow))
          + ", filter=" + (getScanFilter() == null ? null : getScanFilter().toString())
          + ", regionServer=" + regionServer + "]";
    }

  }

  @Override
  public int getOperatorType() {
    return 1001;
  }

}
