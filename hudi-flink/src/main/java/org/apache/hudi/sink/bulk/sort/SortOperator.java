/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk.sort;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator for batch sort.
 *
 * <p>Copied from org.apache.flink.table.runtime.operators.sort.SortOperator to change the annotation.
 *
 * SortOperator用于将一批插入的数据排序后再写入。开启write.bulk_insert.sort_by_partition配置项会启用此特性。
 */
public class SortOperator extends TableStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private static final Logger LOG = LoggerFactory.getLogger(SortOperator.class);

  private GeneratedNormalizedKeyComputer gComputer;
  private GeneratedRecordComparator gComparator;

  private transient BinaryExternalSorter sorter;
  private transient StreamRecordCollector<RowData> collector;
  private transient BinaryRowDataSerializer binarySerializer;

  public SortOperator(
      GeneratedNormalizedKeyComputer gComputer, GeneratedRecordComparator gComparator) {
    this.gComputer = gComputer;
    this.gComparator = gComparator;
  }

  @Override
  public void open() throws Exception {
    super.open();
    LOG.info("Opening SortOperator");

    // 获取用户代码classloader
    ClassLoader cl = getContainingTask().getUserCodeClassLoader();

    // 获取RowData序列化器
    AbstractRowDataSerializer inputSerializer =
        (AbstractRowDataSerializer)
            getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
    // 创建Hudi专用的序列化器，传入参数为RowData字段数
    this.binarySerializer = new BinaryRowDataSerializer(inputSerializer.getArity());

    NormalizedKeyComputer computer = gComputer.newInstance(cl);
    RecordComparator comparator = gComparator.newInstance(cl);
    gComputer = null;
    gComparator = null;

    // 获取作业的内存管理器
    MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
    // 使用Flink提供的二进制MergeSort工具对RowData排序
    this.sorter =
        new BinaryExternalSorter(
            this.getContainingTask(),
            memManager,
            computeMemorySize(),
            this.getContainingTask().getEnvironment().getIOManager(),
            inputSerializer,
            binarySerializer,
            computer,
            comparator,
            getContainingTask().getJobConfiguration());
    // 排序工具包含了排序线程，合并线程以及溢写Thread，该方法启动这些线程
    this.sorter.startThreads();

    // 创建结果收集器，用于发送结果到下游
    collector = new StreamRecordCollector<>(output);

    // register the the metrics.
    // 创建监控仪表，包含内存已用字节数，溢写文件数和溢写字节数
    getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
    getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
    getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    //每次接收到一个RowData类型数据，都把它放入BinaryExternalSorter的缓存中
    this.sorter.write(element.getValue());
  }

  @Override
  public void endInput() throws Exception {
    //当一批数据插入过程结束时，SortOperator将sorter中以排序的二进制RowData数据顺序取出，发往下游。
    BinaryRowData row = binarySerializer.createInstance();
    MutableObjectIterator<BinaryRowData> iterator = sorter.getIterator();
    while ((row = iterator.next(row)) != null) {
      collector.collect(row);
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing SortOperator");
    super.close();
    if (sorter != null) {
      sorter.close();
    }
  }
}
