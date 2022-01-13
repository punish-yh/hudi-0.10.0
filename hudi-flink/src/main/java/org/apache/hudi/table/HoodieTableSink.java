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

package org.apache.hudi.table;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final Configuration conf;
  private final ResolvedSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, ResolvedSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  public HoodieTableSink(Configuration conf, ResolvedSchema schema, boolean overwrite) {
    this.conf = conf;
    this.schema = schema;
    this.overwrite = overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    //和下面的lambda表达式写法相同
//    return new DataStreamSinkProvider() {
//      @Override
//      public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
//        return null;
//      }
//    };
    return (DataStreamSinkProvider) dataStream -> {

      // setup configuration
      // 配置ckp 超时时间
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      // 设置Hudi的instant commit超时时间为Flink的checkpoint超时时间
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

      // 获取schema对应每列数据类型
      RowType rowType = (RowType) schema.toSourceRowDataType().notNull().getLogicalType();

      // bulk_insert mode
      /**
       * 获取写入的操作类型，默认upsert，如果是 bulk_insert 则单独处理。这个参数可以在with中设置
       * 支持的类型和说明见 https://hudi.apache.org/docs/write_operations/#
       */
      final String writeOperation = this.conf.get(FlinkOptions.OPERATION);
      if (WriteOperationType.fromValue(writeOperation) == WriteOperationType.BULK_INSERT) {
        return context.isBounded() ? Pipelines.bulkInsert(conf, rowType, dataStream) : Pipelines.append(conf, rowType, dataStream);
      }

      // Append mode
      //MOR不是append mode
      if (OptionsResolver.isAppendMode(conf)) {
        return Pipelines.append(conf, rowType, dataStream);
      }

      // default parallelism
      int parallelism = dataStream.getExecutionConfig().getParallelism();
      DataStream<Object> pipeline;

      // bootstrap
      final DataStream<HoodieRecord> hoodieRecordDataStream =
          Pipelines.bootstrap(conf, rowType, parallelism, dataStream, context.isBounded(), overwrite);
      // write pipeline
      pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
      // compaction
      // 是否压缩 （表类型为MERGE_ON_READ，并且启用了异步压缩）
      if (StreamerUtil.needsAsyncCompaction(conf)) {
        //压缩操作自带clean
        return Pipelines.compact(conf, pipeline);
      } else {
        //不压缩的话只做clean
        return Pipelines.clean(conf, pipeline);
      }
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    if (conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED)) {
      return ChangelogModes.FULL;
    } else {
      return ChangelogModes.UPSERT;
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.schema, this.overwrite);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partitions) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite && partitions.size() > 0) {
      this.conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    }
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    // set up the operation as INSERT_OVERWRITE_TABLE first,
    // if there are explicit partitions, #applyStaticPartition would overwrite the option.
    this.conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE_TABLE.value());
  }
}
