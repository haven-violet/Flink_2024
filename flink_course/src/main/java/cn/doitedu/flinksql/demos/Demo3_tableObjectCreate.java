package cn.doitedu.flinksql.demos;

import com.alibaba.fastjson.JSON;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class Demo3_tableObjectCreate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //假设 table_a已经被创建过
//        tenv.executeSql("create table table_a(id int, name string) " +
//                "with ()" );
        /**
         * 一、从一个已存在的表名，来创建Table对象
         */
//        Table table_a = tenv.from("table_a");

        /**
         * 二、从一个TableDescriptor来创建Table对象
         */
        tenv.from(TableDescriptor
                .forConnector("kafka") //指定连接器
                .schema(Schema.newBuilder() //指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.INT())
                        .build()
                )
                .format("json") //指定数据源的数据格式
                .option("topic", "doit30-3") // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "192.168.157.102:9092")
                .option("properties.group.id", "g1")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());

        /**
         * 三、从数据流来创建table对象
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.157.102:9092")
                .setTopics("doit30-3")
                .setGroupId("g2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
/*
+----+--------------------------------+
| op |                             f0 |
+----+--------------------------------+
| +I | {"id":1, "name":"zs", "age"... |
| +I | {"id":1, "name":"xh", "age"... |
| +I | {"id":1, "name":"lisi", "ag... |
| +I | {"id":1, "name":"xq", "age"... |

 */
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");
        //3.1 不指定schema, 将流创建成Table对象, 表的schema是默认的，往往不符合我们的要求
        Table table1 = tenv.fromDataStream(kafkaStream);
        //table1.execute().print();


        //3.2 为了获得更理想的结构, 可以先把数据流中的数据转换成javabean类型
        SingleOutputStreamOperator<Person> javaBeanStream = kafkaStream.map(json -> JSON.parseObject(json, Person.class));
        Table table2 = tenv.fromDataStream(javaBeanStream);
        //table2.execute().print();

        //3.3 手动指定schema定义, 来将一个javaBean流, 转换成Table对象
        Table table3 = tenv.fromDataStream(javaBeanStream, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("gender", DataTypes.STRING())
                .build());
        //table3.execute().print();

        /**
         * 四、用测试数据来得到一个对象
         */
        //4.1 单字段数据建测试表
        Table table4 = tenv.fromValues(1, 2, 3, 4, 5);
        //table4.printSchema();
        //table4.execute().print();

        //4.2 多字段数据建测试表
        Table table5 = tenv.fromValues(
                TableSchema.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.DOUBLE())
                        .build(),
                Row.of(1, "zs",  18.2),
                Row.of(2, "bb",  28.2),
                Row.of(3, "cc",  16.2),
                Row.of(4, "zs", 38.2)
        );
        table5.printSchema();
        table5.execute().print();
    }

    @AllArgsConstructor
    @NoArgsConstructor
    public static class Person{
        public int id;
        public String name;
        public int age;
        public String gender;
    }
}

