package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 */
public class Demo11_UpsertKafkaConnectorTest2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9998);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });
        SingleOutputStreamOperator<Bean2> bean2 = s2.map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });

        //流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);

        //创建upsert-kafka 连接器目标表
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

        select bean1.id, bean1.gender, bean2.name
        from bean1 left join bean2 on bean1.id = bean2.id

         */
        tenv.executeSql("create table t_upsert_kafka (\n" +
                "            id int primary key not enforced,\n" +
                "            gender string,\n" +
                "            name string\n" +
                "        ) with (\n" +
                "            'connector' = 'upsert-kafka',\n" +
                "            'topic' = 'doit30-upsert2',\n" +
                "            'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                "            'key.format' = 'csv',\n" +
                "            'value.format' = 'csv'\n" +
                "        )");

        /**
         * +i
         * -d +i
         *
         * 1,male,
         * null
         * 1,male,zs
         *
         */
        tenv.executeSql(" insert into t_upsert_kafka select bean1.id, bean1.gender, bean2.name\n" +
                "        from bean1 left join bean2 on bean1.id = bean2.id");



    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean1 {
        private int id;
        private String gender;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean2 {
        private int id;
        private String name;
    }
}
