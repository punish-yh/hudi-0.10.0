package org.apache.hudi.intsig;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProducerData {

    /**
     * 生成数据，写到本地文件中
     * @param args
     */
    public static void main(String[] args) {

        int checkpoint = 3;

        int checkInterval = 4;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(1000 * checkpoint);

        tableEnv.executeSql("CREATE TABLE hudi_partners_1(\n" +
                "    id BIGINT,\n" +
                "    eid STRING,\n" +
                "    seq_no INT,\n" +
                "    PRIMARY KEY(id) NOT ENFORCED\n" +
                ")WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/home/temp/flink-hudi/hudi_partners_1',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'changelog.enabled' = 'true',\n" + //开启changelog模式，因为MOR处理delete有语义缺陷
                "    'compaction.max_memory' = '512'," + //压缩时最大使用内存数
                "    'read.streaming.check-interval' = '" + checkInterval + "'\n" +
                ")");

        //循环造数据
        for(int i = 0; i < 10000; i ++){
            tableEnv.executeSql("INSERT INTO hudi_partners_1 VALUES('" + i + "', '" + (i + ("abc")) + "', 370402)");
        }

    }
}
