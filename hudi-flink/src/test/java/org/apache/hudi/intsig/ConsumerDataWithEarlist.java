package org.apache.hudi.intsig;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 从头消费数据，必须配合Producer使用
 */
public class ConsumerDataWithEarlist {

    public static void main(String[] args) {
        int checkpoint = 3;


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(1000 * checkpoint);

        //只读
        tableEnv.executeSql("CREATE TABLE hudi_partners_1(\n" +
                "    id BIGINT,\n" +
                "    eid STRING,\n" +
                "    seq_no INT,\n" +
                "    PRIMARY KEY(id) NOT ENFORCED\n" +
                ")WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/home/temp/flink-hudi/hudi_partners_1',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'read.streaming.enabled' = 'true',\n" + //默认就是false，读最新全量快照
                //"    'changelog.enabled' = 'true',\n" + //开启changelog模式，保留所有的变更（I/-U/U/D）
                //开启chagelog后就不能sink到一般的kafka了，必须是upsert-kafka才可以
                "    'read.start-commit' = 'earliest' \n" + //从头消费数据
                ")");

        tableEnv.executeSql("select * from hudi_partners_1");
    }
}
