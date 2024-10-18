package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo11_UpsertKafkaConnectorTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 9999);

        SingleOutputStreamOperator<Bean1> bean1 = source.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });
        //流转表
        tenv.createTemporaryView("bean1", bean1);
        //创建目标kafka映射表
        // todo 会产生 -U +U +I 的操作的数据流
        //tenv.executeSql("select gender, count(1) as cnt from bean1 group by gender").print();
        /*
        create table t_upsert_kafka (
            gender string primary key not enforced,
            cnt bigint
        ) with (
            'connector' = 'upsert-kafka',
            'topic' = 'doit30-upsert',
            'properties.bootstrap.servers' = '192.168.157.102:9092',
            'key.format' = 'csv',
            'value.format' = 'csv'
        )
         */
        tenv.executeSql("create table t_upsert_kafka (\n" +
                "            gender string primary key not enforced,\n" +
                "            cnt bigint \n" +
                "        ) with (\n" +
                "            'connector' = 'upsert-kafka',\n" +
                "            'topic' = 'doit30-upsert',\n" +
                "            'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                "            'key.format' = 'csv',\n" +
                "            'value.format' = 'csv'\n" +
                "        )");
        //查询每种性别的数据行数, 并将结果插入到目标表
        tenv.executeSql("insert into t_upsert_kafka" +
                " select gender, count(1) as cnt from bean1 group by gender");

        /*
        Query schema: [gender: STRING, cnt: BIGINT NOT NULL]
        Sink schema:  [gender: STRING, cnt: INT]

+----+--------------------------------+----------------------+
| op |                         gender |                  cnt |
+----+--------------------------------+----------------------+
| +I |                           male |                    1 |
| +I |                         female |                    1 |
| -U |                           male |                    1 |
| +U |                           male |                    2 |

upsert kafka中存储真实数据，但是在flinkSql消费upsert-kafka的时候，它会把原来changelogStream还原出来
male,1
female,1
male,2
         */
        tenv.executeSql("select * from t_upsert_kafka").print();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        private int id;
        private String gender;
    }
}
