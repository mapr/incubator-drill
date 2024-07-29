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
package org.apache.drill.exec.store.mapr.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mapr.db.impl.IdCodec;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.physical.impl.join.RowKeyJoin;
import org.apache.drill.exec.record.AbstractRecordBatch.BatchState;
import org.apache.drill.exec.vector.ValueVector;

import java.nio.ByteBuffer;

/**
 * A RestrictedMapRDBSubScanSpec encapsulates a join instance which contains the ValueVectors of row keys and
 * is associated with this sub-scan and also exposes an iterator type interface over the row key vectors.
 */
public class RestrictedMapRDBSubScanSpec extends MapRDBSubScanSpec {

  /**
   * The RowKeyJoin instance (specific to one minor fragment) which will supply this
   * subscan with the set of rowkeys. For efficiency, we keep a reference to this
   * join rather than making another copy of the rowkeys.
   */
  private RowKeyJoin rjbatch = null;

  /**
   * The following are needed to maintain internal state of iteration over the set
   * of row keys
   */
  private ValueVector rowKeyVector = null; // the current row key value vector
  private int currentIndex = 0;  // the current index within the row key vector
  private int maxOccupiedIndex = -1; // max occupied index within a row key vector

  public RestrictedMapRDBSubScanSpec(String tableName, String regionServer, byte[] serializedFilter, String userName) {
    super(tableName, null, regionServer, null, null, serializedFilter, null, userName);
  }
  /* package */ RestrictedMapRDBSubScanSpec() {
    // empty constructor, to be used with builder pattern;
  }

  public void setJoinForSubScan(RowKeyJoin rjbatch) {
    this.rjbatch = rjbatch;
  }

  @JsonIgnore
  public RowKeyJoin getJoinForSubScan() {
    return rjbatch;
  }

  /**
   * Initializes the row key vector with a row key batch from the {@link RowKeyJoin}
   *
   * @return {@code true} if {@link RowKeyJoin} had the batch to use for the initialization
   */
  @JsonIgnore
  private boolean init() {
    if (rjbatch != null) {
      Pair<ValueVector, Integer> currentRowKeyBatch = rjbatch.nextRowKeyBatch();

      if (currentRowKeyBatch != null) {
        this.rowKeyVector = currentRowKeyBatch.getLeft();
        this.maxOccupiedIndex = currentRowKeyBatch.getRight();
        this.currentIndex = 0;
        return true;
      }
    }
    return false;
  }

  /**
   * Return {@code true} if the row key join is in the build schema phase
   */
  @JsonIgnore
  public boolean isBuildSchemaPhase() {
    return rjbatch.getBatchState() == BatchState.BUILD_SCHEMA;
  }

  /**
   * @return {@code true} if the iteration has row keys to process
   */
  @JsonIgnore
  public boolean hasRowKeys() {
    if (rowKeyVector != null) {
      return true;
    }
    return init();
  }

  /**
   * Returns ids of rowKeys to be read.
   * Number of rowKey ids returned will be numRowKeysToRead at the most i.e. it
   * will be less than numRowKeysToRead if only that many exist in the currentBatch.
   */
  @JsonIgnore
  public ByteBuffer[] getRowKeyIdsToRead(int numRowKeysToRead) {
    if (hasRowKeys()) {
      int numKeys = Math.min(numRowKeysToRead, getLeftRowKeys());
      if (numKeys > 0) {
        int index = 0;
        final ByteBuffer[] rowKeyIds = new ByteBuffer[numKeys];

        while (index < numKeys) {
          Object o = rowKeyVector.getAccessor().getObject(currentIndex + index);
          rowKeyIds[index++] = IdCodec.encode(o.toString());
        }

        updateRowKeysRead(numKeys);
        return rowKeyIds;
      }
    }
    return null;
  }

  /**
   * Updates the index to reflect number of keys read
   *
   * @param numKeys keys was read
   */
  @JsonIgnore
  private void updateRowKeysRead(int numKeys) {
    if (maxOccupiedIndex != -1) {
      currentIndex += numKeys;
      if (currentIndex > maxOccupiedIndex + 1) {
        throw new DrillRuntimeException("Vector index out of bounds exception");
      }
      if (currentIndex == maxOccupiedIndex + 1) {
        resetRowKeyVector();
      }
    }
  }

  private void resetRowKeyVector() {
    rowKeyVector = null;
    currentIndex = 0;
    maxOccupiedIndex = -1;
  }

  /**
   * @return number of row keys left in the current vector
   */
  @JsonIgnore
  private int getLeftRowKeys() {
    return maxOccupiedIndex - currentIndex + 1;
  }
}
