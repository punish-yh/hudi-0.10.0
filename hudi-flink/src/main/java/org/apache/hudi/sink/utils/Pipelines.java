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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperator;
import org.apache.hudi.sink.append.AppendWriteOperator;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.bootstrap.batch.BatchBootstrapOperator;
import org.apache.hudi.sink.bulk.BulkInsertWriteOperator;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.common.WriteOperatorFactory;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities to generate all kinds of sub-pipelines.
 */
public class Pipelines {

  public static DataStreamSink<Object> bulkInsert(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    // 创建出批量插入operator工厂类
    WriteOperatorFactory<RowData> operatorFactory = BulkInsertWriteOperator.getFactory(conf, rowType);
    // 获取分区字段
    final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
    if (partitionFields.length > 0) {
      // 创建出key生成器，用于指定数据分组，keyBy算子使用
      RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
      // 如果启用write.bulk_insert.shuffle_by_partition
      // 是否根据分区字段分发数据。启用该参数可以减少小文件，但可能存在数据倾斜
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_BY_PARTITION)) {

        // shuffle by partition keys
        dataStream = dataStream.keyBy(rowDataKeyGen::getPartitionPath);
      }

      // 如果需要按照分区排序（排序使得数据具有更好的聚集性）
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_BY_PARTITION)) {
        // 创建一个排序operator
        SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, partitionFields);
        // sort by partition keys
        // 为datastream增加一个排序操作符
        dataStream = dataStream
            .transform("partition_key_sorter",
                TypeInformation.of(RowData.class),
                sortOperatorGen.createSortOperator())
            .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
        ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
            conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
      }
    }
    //为dataStream加入批量写入operator并返回
    return dataStream
        .transform("hoodie_bulk_insert_write",
            TypeInformation.of(Object.class),
            operatorFactory)
        // follow the parallelism of upstream operators to avoid shuffle
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS))
        .addSink(DummySink.INSTANCE)
        .name("dummy");
  }

  public static DataStreamSink<Object> append(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    WriteOperatorFactory<RowData> operatorFactory = AppendWriteOperator.getFactory(conf, rowType);

    return dataStream
        .transform("hoodie_append_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS))
        .addSink(DummySink.INSTANCE)
        .name("dummy");
  }

  /**
   * Constructs bootstrap pipeline as streaming.
   */
  public static DataStream<HoodieRecord> bootstrap(
      Configuration conf,
      RowType rowType,
      int defaultParallelism,
      DataStream<RowData> dataStream) {
    return bootstrap(conf, rowType, defaultParallelism, dataStream, false, false);
  }

  /**
   * Constructs bootstrap pipeline.
   *
   * @param conf The configuration
   * @param rowType The row type
   * @param defaultParallelism The default parallelism
   * @param dataStream The data stream
   * @param bounded Whether the source is bounded
   * @param overwrite Whether it is insert overwrite
   */
  public static DataStream<HoodieRecord> bootstrap(
      Configuration conf,
      RowType rowType,
      int defaultParallelism,
      DataStream<RowData> dataStream,
      boolean bounded,
      boolean overwrite) {
    final boolean globalIndex = conf.getBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED);
    if (overwrite) {
      return rowDataToHoodieRecord(conf, rowType, dataStream);
    } else if (bounded && !globalIndex && OptionsResolver.isPartitionedTable(conf)) {
      //有界 无全局索引 非分区表
      return boundedBootstrap(conf, rowType, defaultParallelism, dataStream);
    } else {
      return streamBootstrap(conf, rowType, defaultParallelism, dataStream, bounded);
    }
  }

  /**
   * 非 overwrite 且为流模式时构建的DataStream
   * @param conf
   * @param rowType
   * @param defaultParallelism
   * @param dataStream
   * @param bounded
   * @return
   */
  private static DataStream<HoodieRecord> streamBootstrap(
      Configuration conf,
      RowType rowType,
      int defaultParallelism,
      DataStream<RowData> dataStream,
      boolean bounded) {
    DataStream<HoodieRecord> dataStream1 = rowDataToHoodieRecord(conf, rowType, dataStream);

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED) || bounded) {
      // 有索引 或 有界
      //如果启用，会在启动时自动加载索引，包装为IndexRecord发往下游
      dataStream1 = dataStream1.transform(
              "index_bootstrap",
              TypeInformation.of(HoodieRecord.class),
              new BootstrapOperator<>(conf))
          .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(defaultParallelism))
          .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
    }

    return dataStream1;
  }

  private static DataStream<HoodieRecord> boundedBootstrap(
      Configuration conf,
      RowType rowType,
      int defaultParallelism,
      DataStream<RowData> dataStream) {
    final RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
    // shuffle by partition keys
    dataStream = dataStream
        .keyBy(rowDataKeyGen::getPartitionPath);

    return rowDataToHoodieRecord(conf, rowType, dataStream)
        .transform(
            "batch_index_bootstrap",
            TypeInformation.of(HoodieRecord.class),
            new BatchBootstrapOperator<>(conf))
        .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(defaultParallelism))
        .uid("uid_batch_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
  }

  public static DataStream<HoodieRecord> rowDataToHoodieRecord(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    return dataStream.map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class));
  }

  /**
   *
   * @param conf
   * @param defaultParallelism
   * @param dataStream
   * @return
   */
  public static DataStream<Object> hoodieStreamWrite(Configuration conf, int defaultParallelism, DataStream<HoodieRecord> dataStream) {
    WriteOperatorFactory<HoodieRecord> operatorFactory = StreamWriteOperator.getFactory(conf);
    return dataStream
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        //
            /**
             *  目的：按照key进行分组，防止在同一时间多个子任务写入同一个bucket
             *  步骤：1. keyby
             *  2. 每有一个数据进入算子，则会触发一次BucketAssignFunction.processElement() （更新index以及处理数据location问题）
             *  3. 按照location keyBy 然后批量写入？
             */
        .keyBy(HoodieRecord::getRecordKey)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner_" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getOptional(FlinkOptions.BUCKET_ASSIGN_TASKS).orElse(defaultParallelism))
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
  }

  /**
   * 需要异步压缩的场景
   *
   * 首先在coordinator通知checkpoint完毕的时候生成压缩计划
   * 然后使用CompactFunction压缩hudi table数据
   *
   * @param conf
   * @param dataStream
   * @return
   */
  public static DataStreamSink<CompactionCommitEvent> compact(Configuration conf, DataStream<Object> dataStream) {
    return dataStream.transform("compact_plan_generate",
        TypeInformation.of(CompactionPlanEvent.class),
        new CompactionPlanOperator(conf))//没有process代码，而是在ckp完成后调用 scheduleCompaction
        .setParallelism(1) // plan generate must be singleton
        .rebalance() // 将数据打乱，轮询发送到下游
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(conf)))
        .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
        .addSink(new CompactionCommitSink(conf))
        .name("compact_commit")
        .setParallelism(1); // compaction commit should be singleton
  }

  /**
   * 需要同同步压缩的场景
   * 单独创建一个sink，这个sink只给一个并行度
   * @param conf
   * @param dataStream
   * @return
   */
  public static DataStreamSink<Object> clean(Configuration conf, DataStream<Object> dataStream) {
    return dataStream.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits");
  }

  /**
   * Dummy sink that does nothing.
   */
  public static class DummySink implements SinkFunction<Object> {
    private static final long serialVersionUID = 1L;
    public static DummySink INSTANCE = new DummySink();
  }
}
