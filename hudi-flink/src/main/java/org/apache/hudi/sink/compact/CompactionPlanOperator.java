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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkTables;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Operator that generates the compaction plan with pluggable strategies on finished checkpoints.
 *
 * <p>It should be singleton to avoid conflicts.
 */
public class CompactionPlanOperator extends AbstractStreamOperator<CompactionPlanEvent>
    implements OneInputStreamOperator<Object, CompactionPlanEvent> {

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Meta Client.
   */
  @SuppressWarnings("rawtypes")
  private transient HoodieFlinkTable table;

  public CompactionPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.table = FlinkTables.createTable(conf, getRuntimeContext());
    // when starting up, rolls back all the inflight compaction instants if there exists,
    // these instants are in priority for scheduling task because the compaction instants are
    // scheduled from earliest(FIFO sequence).
    CompactionUtil.rollbackCompaction(table);
  }

  @Override
  public void processElement(StreamRecord<Object> streamRecord) {
    // no operation
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    try {
      table.getMetaClient().reloadActiveTimeline();
      // There is no good way to infer when the compaction task for an instant crushed
      // or is still undergoing. So we use a configured timeout threshold to control the rollback:
      // {@code FlinkOptions.COMPACTION_TIMEOUT_SECONDS},
      // when the earliest inflight instant has timed out, assumes it has failed
      // already and just rolls it back.

      /**
       * 没有好的方法可以推断出compaction 任务何时被关闭或仍在进行中。
       * 所以我们使用配置的超时阈值来控制回滚：{@code FlinkOptions.COMPACTION_TIMEOUT_SECONDS}，
       * 当最早的压缩超时，则认为它已经失败只是回滚。
       */

      // comment out: do we really need the timeout rollback ?
      // CompactionUtil.rollbackEarliestCompaction(table, conf);
      // schedule一个新的压缩操作
      scheduleCompaction(table, checkpointId);
    } catch (Throwable throwable) {
      // make it fail-safe
      LOG.error("Error while scheduling compaction plan for checkpoint: " + checkpointId, throwable);
    }
  }

  private void scheduleCompaction(HoodieFlinkTable<?> table, long checkpointId) throws IOException {
    // the last instant takes the highest priority.
    // 获取最近一个活跃的可被压缩的instant
    Option<HoodieInstant> firstRequested = table.getActiveTimeline().filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED).firstInstant();
    if (!firstRequested.isPresent()) {
      // do nothing.
      LOG.info("No compaction plan for checkpoint " + checkpointId);
      return;
    }

    // 获取这个instant的时间
    String compactionInstantTime = firstRequested.get().getTimestamp();

    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);
// 如果当前正在压缩的instant时间和最近一个活跃的可被压缩的instant时间相同
    // 说明schedule的compact操作重复了
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      // do nothing.
      LOG.info("Empty compaction plan for instant " + compactionInstantTime);
    } else {
      // 获取要压缩的instant
      HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      // 标记该instant状态为inflight（正在处理）
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      // 创建压缩操作
      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
      LOG.info("Execute compaction plan for instant {} as {} file groups", compactionInstantTime, operations.size());
      // 逐个发送压缩操作到下游
      for (CompactionOperation operation : operations) {
        output.collect(new StreamRecord<>(new CompactionPlanEvent(compactionInstantTime, operation)));
      }
    }
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionPlanEvent>> output) {
    this.output = output;
  }
}
