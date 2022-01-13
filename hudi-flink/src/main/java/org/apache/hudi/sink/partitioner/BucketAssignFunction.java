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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bootstrap.IndexRecord;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>All the records are tagged with HoodieRecordLocation, instead of real instant time,
 * INSERT record uses "I" and UPSERT record uses "U" as instant time. There is no need to keep
 * the "real" instant time for each record, the bucket ID (partition path & fileID) actually decides
 * where the record should write to. The "I" and "U" tags are only used for downstream to decide whether
 * the data bucket is an INSERT or an UPSERT, we should factor the tags out when the underneath writer
 * supports specifying the bucket type explicitly.
 *
 * <p>The output records should then shuffle by the bucket ID and thus do scalable write.
 *
 * @see BucketAssigner
 */
public class BucketAssignFunction<K, I, O extends HoodieRecord<?>>
    extends KeyedProcessFunction<K, I, O>
    implements CheckpointedFunction, CheckpointListener {

  /**
   * Index cache(speed-up) state for the underneath file based(BloomFilter) indices.
   * When a record came in, we do these check:
   *
   * <ul>
   *   <li>Try to load all the records in the partition path where the record belongs to</li>
   *   <li>Checks whether the state contains the record key</li>
   *   <li>If it does, tag the record with the location</li>
   *   <li>If it does not, use the {@link BucketAssigner} to generate a new bucket ID</li>
   * </ul>
   */
  private ValueState<HoodieRecordGlobalLocation> indexState;

  /**
   * Bucket assigner to assign new bucket IDs or reuse existing ones.
   */
  private BucketAssigner bucketAssigner;

  private final Configuration conf;

  private final boolean isChangingRecords;

  /**
   * Used to create DELETE payload.
   */
  private PayloadCreation payloadCreation;

  /**
   * If the index is global, update the index for the old partition path
   * if same key record with different partition path came in.
   */
  private final boolean globalIndex;

  public BucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
    this.globalIndex = conf.getBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED)
        && !conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    HoodieWriteConfig writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(StreamerUtil.getHadoopConf()),
        new FlinkTaskContextSupplier(getRuntimeContext()));
    this.bucketAssigner = BucketAssigners.create(
        getRuntimeContext().getIndexOfThisSubtask(),
        getRuntimeContext().getMaxNumberOfParallelSubtasks(),
        getRuntimeContext().getNumberOfParallelSubtasks(),
        ignoreSmallFiles(),
        HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)),
        context,
        writeConfig);
    this.payloadCreation = PayloadCreation.instance(this.conf);
  }

  private boolean ignoreSmallFiles() {
    WriteOperationType operationType = WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
    return WriteOperationType.isOverwrite(operationType);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    this.bucketAssigner.reset();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    ValueStateDescriptor<HoodieRecordGlobalLocation> indexStateDesc =
        new ValueStateDescriptor<>(
            "indexState",
            TypeInformation.of(HoodieRecordGlobalLocation.class));
    double ttl = conf.getDouble(FlinkOptions.INDEX_STATE_TTL) * 24 * 60 * 60 * 1000;
    if (ttl > 0) {
      indexStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds((long) ttl)).build());
    }
    indexState = context.getKeyedStateStore().getState(indexStateDesc);
  }

  @Override
  public void processElement(I value, Context ctx, Collector<O> out) throws Exception {
    /**
     * 执行数据分桶操作，为每一条数据分配它的存储位置。如果开启了索引则会将索引数据加载到operator状态缓存中
     *
     * 如果该条数据是条 indexRecord数据则更新index
     * 更新recordKey和储存位置的对应关系
     */
    if (value instanceof IndexRecord) {
      IndexRecord<?> indexRecord = (IndexRecord<?>) value;
      // 更新indexState为索引数据对应的位置
      // 将IndexRecord携带的recordKey和location信息对应存入indexState中
      this.indexState.update((HoodieRecordGlobalLocation) indexRecord.getCurrentLocation());
    } else {
      // 进入此分支则上面接收到的的 HoodieRecord，开始处理数据过程
      processRecord((HoodieRecord<?>) value, out);
    }
  }

  /**
   * 实际上做了如下几件事：
   * 1. 更新index
   * 2. 如果是一条变更record还要删除原数据、更新BucketAssigner
   * 3. insert数据就直接创建一个record写入分配好的分区
   * 4. 这个process主要是根据数据更新索引
   *
   * 其中第二步并不是原子的，如果有删除原分区数据的需求则会先给下游发一个deleteRecord，再发送正常数据。
   * 如果程序delete成功还没来得及insert新数据则有极低的概率导致该数据被删除（exactly-once模式下重启会继续insert，否则会丢数据）
   *
   * @param record
   * @param out
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private void processRecord(HoodieRecord<?> record, Collector<O> out) throws Exception {
    // 1. put the record into the BucketAssigner;
    // 2. look up the state for location, if the record has a location, just send it out;
    // 3. if it is an INSERT, decide the location using the BucketAssigner then send it out.

    // 1. 将数据放入 BucketAssigner
    // 2. 查找该条数据的location，如果有location则发送到location（更新删除操作）
    // 3. 如果是insert数据则location由 BucketAssigner 决定

    // 获取HoodieKey，分别拿出recordKey和partitionPath
    final HoodieKey hoodieKey = record.getKey();
    final String recordKey = hoodieKey.getRecordKey();
    final String partitionPath = hoodieKey.getPartitionPath();
    // 封装了HoodieRecord的存储位置，即这条HoodieRecord对应哪个文件
    final HoodieRecordLocation location;

    // Only changing records need looking up the index for the location,
    // append only records are always recognized as INSERT.
    // 获取index中保存的location信息
    HoodieRecordGlobalLocation oldLoc = indexState.value();
    // 如果操作类型为UPSERT，DELETE或者UPSERT_PREPPED，isChangingRecords为true
    if (isChangingRecords && oldLoc != null) {
      // Set up the instant time as "U" to mark the bucket as an update bucket.
      // 如果index的partitionPath和当前HoodieRecord的不同
      if (!Objects.equals(oldLoc.getPartitionPath(), partitionPath)) {
        if (globalIndex) {
          // if partition path changes, emit a delete record for old partition path,
          // then update the index state using location with new partition path.
          // 如果分区路径改变了则先删除原来分区的数据再更新到新分区路径

          // 创建一个删除元素发给下游，删除老的partitionPath信息
          // TODO 老分区删除数据和新分区写入数据之间是不是原子的？如果不是原子的则无法保证数据一致性（很可能数据已经删除了但新数据还未写入时程序宕机。则下游甚至连老数据都拿不到了）
          HoodieRecord<?> deleteRecord = new HoodieRecord<>(new HoodieKey(recordKey, oldLoc.getPartitionPath()),
              payloadCreation.createDeletePayload((BaseAvroPayload) record.getData()));
          deleteRecord.setCurrentLocation(oldLoc.toLocal("U"));
          deleteRecord.seal();
          // 向下游传送 deleteRecord
          out.collect((O) deleteRecord);
        }
        // 通过BucketAssigner获取新的存储位置
        location = getNewRecordLocation(partitionPath);
        // 更新IndexState为新的partitionPath和location
        updateIndexState(partitionPath, location);
      } else {
        // 加入更新数据的位置信息到bucketAssigner
        location = oldLoc.toLocal("U");
        this.bucketAssigner.addUpdate(partitionPath, location.getFileId());
      }
    } else {
      // 如果不是数据更新操作
      location = getNewRecordLocation(partitionPath);
    }
    // always refresh the index
    // 确保数据更新操作刷新索引（indexState）
    if (isChangingRecords) {
      updateIndexState(partitionPath, location);
    }
    //更新record当前location，然后原封不动传送到下游
    record.setCurrentLocation(location);
    out.collect((O) record);
  }

  private HoodieRecordLocation getNewRecordLocation(String partitionPath) {
    final BucketInfo bucketInfo = this.bucketAssigner.addInsert(partitionPath);
    final HoodieRecordLocation location;
    switch (bucketInfo.getBucketType()) {
      case INSERT:
        // This is an insert bucket, use HoodieRecordLocation instant time as "I".
        // Downstream operators can then check the instant time to know whether
        // a record belongs to an insert bucket.
        location = new HoodieRecordLocation("I", bucketInfo.getFileIdPrefix());
        break;
      case UPDATE:
        location = new HoodieRecordLocation("U", bucketInfo.getFileIdPrefix());
        break;
      default:
        throw new AssertionError();
    }
    return location;
  }

  private void updateIndexState(
      String partitionPath,
      HoodieRecordLocation localLoc) throws Exception {
    this.indexState.update(HoodieRecordGlobalLocation.fromLocal(partitionPath, localLoc));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    // Refresh the table state when there are new commits.
    this.bucketAssigner.reload(checkpointId);
  }

  @Override
  public void close() throws Exception {
    this.bucketAssigner.close();
  }
}
