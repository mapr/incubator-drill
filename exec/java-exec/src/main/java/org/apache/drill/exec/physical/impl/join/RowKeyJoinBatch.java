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
package org.apache.drill.exec.physical.impl.join;


import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.RowKeyJoinPOP;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowKeyJoinBatch extends AbstractRecordBatch<RowKeyJoinPOP> implements RowKeyJoin {
  private static final Logger logger = LoggerFactory.getLogger(RowKeyJoinBatch.class);

  // primary table side record batch
  private final RecordBatch left;

  // index table side record batch
  private final RecordBatch right;

  private boolean hasRowKeyBatch;
  private IterOutcome leftUpstream = IterOutcome.NONE;
  private IterOutcome rightUpstream = IterOutcome.NONE;
  private final List<TransferPair> transfers = Lists.newArrayList();
  private int recordCount;
  private final SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private RowKeyJoinState rkJoinState = RowKeyJoinState.INITIAL;

  public RowKeyJoinBatch(RowKeyJoinPOP config, FragmentContext context, RecordBatch left, RecordBatch right)
    throws OutOfMemoryException {
    super(config, context, true /* need to build schema */);
    this.left = left;
    this.right = right;
    this.hasRowKeyBatch = false;
  }

  @Override
  public int getRecordCount() {
    if (state == BatchState.DONE) {
      return 0;
    }
    return recordCount;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException("RowKeyJoinBatch does not support selection vector");
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException("RowKeyJoinBatch does not support selection vector");
  }

  @Override
  protected void buildSchema() {
    container.clear();

    rightUpstream = next(1, right);

    if (right.getRecordCount() > 0) {
      hasRowKeyBatch = true;
    }

    leftUpstream = next(left);

    if (left.getRecordCount() > 0) {
      outputCurrentLeftBatch();
      callBack.getSchemaChangedAndReset();
    } else {
      for (final VectorWrapper<?> v : left) {
        final TransferPair pair = v.getValueVector().makeTransferPair(
          container.addOrGet(v.getField(), callBack));
        transfers.add(pair);
      }

      container.buildSchema(left.getSchema().getSelectionVectorMode());
    }
    state = BatchState.NOT_FIRST;
  }

  @Override
  public IterOutcome innerNext() {
    switch (rkJoinState) {
      case INITIAL:
        if (!hasRowKeyBatch) {
          processRight();
          return innerNext();
        }
      case DONE:
      case PROCESSING:
        return processLeft();
      default:
        throw new IllegalStateException("Unexpected value: " + rkJoinState);
    }
  }

  private IterOutcome processLeft() {
    leftUpstream = next(left);
    switch (leftUpstream) {
      case NONE:
        container.setRecordCount(0);
        this.recordCount = 0;
        return IterOutcome.NONE;
      case EMIT:
      case OK_NEW_SCHEMA:
      case OK:
        if (left.getRecordCount() > 0) {
          outputCurrentLeftBatch();
          // Check if schema has changed (this is just to guard against potential changes to the
          // output schema by outputCurrentLeftBatch, but in general the leftUpstream status should
          // be sufficient)
          if (callBack.getSchemaChangedAndReset()) {
            return IterOutcome.OK_NEW_SCHEMA;
          }
        } else {
          container.setRecordCount(0);
          this.recordCount = 0;
        }
        return leftUpstream;
      default:
        throw new IllegalStateException(String.format("Unknown state %s.", rightUpstream));
    }
  }

  private void processRight() {
    rightUpstream = next(1, right);
    switch (rightUpstream) {
      case NONE:
        rkJoinState = RowKeyJoinState.DONE;
        break;
      case EMIT:
      case OK_NEW_SCHEMA:
      case OK:
        if (right.getRecordCount() == 0) {
          logger.trace("rowkeyjoin loop when recordCount == 0. rightUpstream {}", rightUpstream);
          processRight();
        }
        hasRowKeyBatch = true;
        break;
      default:
        throw new IllegalStateException(String.format("Unknown state %s.", rightUpstream));
    }
  }

  private void outputCurrentLeftBatch() {
    container.zeroVectors();
    transfers.clear();

    for (final VectorWrapper<?> v : left) {
      final TransferPair pair = v.getValueVector().makeTransferPair(
        container.addOrGet(v.getField(), callBack));
      transfers.add(pair);
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(left.getSchema().getSelectionVectorMode());
    }

    for (TransferPair t : transfers) {
      t.transfer();
    }

    container.setRecordCount(left.getRecordCount());
    this.recordCount = left.getRecordCount();
  }

  @Override  // implement RowKeyJoin interface
  public boolean hasRowKeyBatch() {
    return hasRowKeyBatch;
  }

  @Override  // implement RowKeyJoin interface
  public Pair<ValueVector, Integer> nextRowKeyBatch() {
    if (hasRowKeyBatch) {
      // since entire right row key batch will be returned to the caller, reset
      // the hasRowKeyBatch to false
      hasRowKeyBatch = false;
      VectorWrapper<?> vw = Iterables.get(right, 0);
      ValueVector vv = vw.getValueVector();
      this.rkJoinState = RowKeyJoinState.PROCESSING;
      return Pair.of(vv, right.getRecordCount() - 1);
    }
    return null;
  }

  @Override   // implement RowKeyJoin interface
  public BatchState getBatchState() {
    return state;
  }

  @Override  // implement RowKeyJoin interface
  public void setBatchState(BatchState newState) {
    state = newState;
  }

  @Override
  public void setRowKeyJoinState(RowKeyJoinState newState) {
    this.rkJoinState = newState;
  }

  @Override
  public RowKeyJoinState getRowKeyJoinState() {
    return rkJoinState;
  }

  @Override
  protected void cancelIncoming() {
    left.cancel();
    right.cancel();
  }

  @Override
  public void close() {
    rkJoinState = RowKeyJoinState.DONE;
    super.close();
  }

  @Override
  public void dump() {
    logger.error("RowKeyJoinBatch[container={}, left={}, right={}, hasRowKeyBatch={}, rkJoinState={}]",
      container, left, right, hasRowKeyBatch, rkJoinState);
  }
}
