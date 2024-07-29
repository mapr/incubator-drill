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
package org.apache.drill.exec.store.mapr.db.json;

import com.google.common.base.Stopwatch;
import com.mapr.db.Table;
import com.mapr.db.impl.BaseJsonTable;
import com.mapr.db.impl.IdCodec;
import com.mapr.db.impl.MultiGet;
import com.mapr.db.ojai.DBDocumentReaderBase;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.join.RowKeyJoin;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScanSpec;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.store.mapr.PluginErrorHandler.dataReadError;


public class RestrictedJsonRecordReader extends MaprDBJsonRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(RestrictedJsonRecordReader.class);

  private final int batchSize; // batchSize for rowKey based document get

  private String[] projections = null; // multiGet projections

  public RestrictedJsonRecordReader(MapRDBSubScanSpec subScanSpec,
                                    MapRDBFormatPlugin formatPlugin,
                                    List<SchemaPath> projectedColumns,
                                    FragmentContext context,
                                    int maxRecordsToRead,
                                    TupleMetadata schema) {

    super(subScanSpec, formatPlugin, projectedColumns, context, maxRecordsToRead, schema);
    batchSize = (int) context.getOptions().getOption(ExecConstants.QUERY_ROWKEYJOIN_BATCHSIZE);
    int idx = 0;
    FieldPath[] scannedFields = this.getScannedFields();

    // only populate projections for non-star query (for star, null is interpreted as all fields)
    if (!this.isStarQuery() && scannedFields != null && scannedFields.length > 0) {
      projections = new String[scannedFields.length];
      for (FieldPath path : scannedFields) {
        projections[idx] = path.asPathString();
        ++idx;
      }
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    RestrictedMapRDBSubScanSpec rss = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);
    RowKeyJoin rjBatch = rss.getJoinForSubScan();
    if (rjBatch == null) {
      throw new ExecutionSetupException("RowKeyJoin Batch is not setup for Restricted MapRDB Subscan");
    }

    AbstractRecordBatch.BatchState state = rjBatch.getBatchState();
    if (state == AbstractRecordBatch.BatchState.BUILD_SCHEMA ||
      state == AbstractRecordBatch.BatchState.FIRST) {
      super.setup(context, output);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createStarted();
    RestrictedMapRDBSubScanSpec restrictedScanSpec = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    vectorWriter.allocate();
    vectorWriter.reset();

    if (!restrictedScanSpec.hasRowKeys()) {
      // not ready to get row keys, so we just load a record to initialize schema; only do this
      // when we are in the build schema phase
      if (restrictedScanSpec.isBuildSchemaPhase()) {
        try (DocumentStream dstream = table.find()) {
          DBDocumentReaderBase reader = (DBDocumentReaderBase) dstream.iterator().next().asReader();
          writeDocument(reader, 0);
        }
        vectorWriter.setValueCount(0);
      }
      return 0;
    }

    Table table = super.formatPlugin.getJsonTableCache().getTable(subScanSpec.getTableName(), subScanSpec.getUserName());
    final MultiGet multiGet = new MultiGet((BaseJsonTable) table, condition, false, projections);
    int recordCount = 0;
    ByteBuffer[] rowKeyIds = getRowKeyIds();

    while (rowKeyIds != null) {
      final List<Document> docList = multiGet.doGet(rowKeyIds);
      for (Document document : docList) {
        DBDocumentReaderBase reader = (DBDocumentReaderBase) document.asReader();
        writeDocument(reader, recordCount++);
        decrementRecordsLimit();
      }
      rowKeyIds = getRowKeyIds();
    }

    vectorWriter.setValueCount(recordCount);
    if (!restrictedScanSpec.hasRowKeys() || !recordsLimitIsNotReached()) {
      restrictedScanSpec.getJoinForSubScan().setRowKeyJoinState(RowKeyJoin.RowKeyJoinState.INITIAL);
    }

    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
    return recordCount;
  }

  private void writeDocument(DBDocumentReaderBase reader, int position) {
    try {
      vectorWriter.setPosition(position);
      documentWriter.writeDBDocument(vectorWriter, reader);
    } catch (UserException e) {
      throw UserException.unsupportedError(e)
        .addContext(String.format("Table: %s, document id: '%s'",
          getTable().getPath(),
          IdCodec.asString(reader.getId())))
        .build(logger);
    } catch (SchemaChangeException e) {
      if (getIgnoreSchemaChange()) {
        logger.warn("{}. Dropping the row from result.", e.getMessage());
        logger.debug("Stack trace:", e);
      } else {
        throw dataReadError(logger, e);
      }
    }
  }

  private ByteBuffer[] getRowKeyIds() {
    ByteBuffer[] rowKeyIds = null;
    if (recordsLimitIsNotReached()) {
      RestrictedMapRDBSubScanSpec restrictedScanSpec = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);
      int batchSize = this.batchSize;
      if (hasRecordsLimit()) {
        batchSize = Math.min(this.batchSize, getRecordsLimit());
      }
      rowKeyIds = restrictedScanSpec.getRowKeyIdsToRead(batchSize);
    }
    return rowKeyIds;
  }

  @Override
  public boolean hasNext() {
    RestrictedMapRDBSubScanSpec restrictedSubScanSpec = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    RowKeyJoin joinBatch = restrictedSubScanSpec.getJoinForSubScan();
    if (joinBatch == null) {
      return false;
    }

    boolean hasMore = false;
    AbstractRecordBatch.BatchState joinBatchState = joinBatch.getBatchState();
    RowKeyJoin.RowKeyJoinState joinState = joinBatch.getRowKeyJoinState();

    switch (joinBatchState) {
      case BUILD_SCHEMA:
        hasMore = true;
        break;
      case FIRST:
        if (recordsLimitIsNotReached()) {
          hasMore = true;
        }
        break;
      default:
        if (joinState != RowKeyJoin.RowKeyJoinState.DONE && recordsLimitIsNotReached()) {
          hasMore = true;
        }
    }
    logger.debug("restricted reader hasMore = {}", hasMore);

    return hasMore;
  }

}
