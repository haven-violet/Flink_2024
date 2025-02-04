package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo2_TableApi {
    public static void main(String[] args) {
        //TODO FlinkSql
        //     方式一: 使用table api 方式来写代码;  类似于 spark的 dataFrame
        //纯表环境
        //TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        //混合环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //建表
        Table table = tableEnv.from(TableDescriptor
                .forConnector("kafka") // 指定连接器
                .schema(Schema.newBuilder() // 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json") //指定数据源的数据格式
                .option("topic", "doit30-3") // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "192.168.157.102:9092")
                .option("properties.group.id", "g1")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());

        //查询
//        table.groupBy($("gender"))
//                .select($("gender"), $("age").avg().as("avg_age"))
//                .execute().print();

        /*
        将一个已创建好的table对象, 注册成sql中的视图名
         */
        tableEnv.createTemporaryView("kafka_table", table);
        //然后就可以写sql语句来进行查询了
        tableEnv.executeSql("select gender, avg(age) as avg_age from kafka_table group by gender")
                .print();

    }
}
/*
create table t_kafka (
id int,
name string,
age int,
gender string
)
with (
'connector' = 'kafka',
'topic' = 'doit30-3',
'properties.bootstrap.servers' = '192.168.157.102:9092',
'properties.group.id' = 'g1',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json',
'json.fail-on-missing-field' = 'false',
'json.ignore-parse-errors' = 'true'
)
 */