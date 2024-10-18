package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Schema定义详细示例 (sql DDL语句定义表结构)
 */
public class Demo7_columnDetail_sql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        //建表(数据源表)
        //{"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.executeSql("create table t_person (\n" +
                "    id int,\n" +           // 物理字段
                "    name string,\n" +      // 物理字段
                "    nick string,\n" +
                "    age int,\n" +
                "    gender string,\n" +
                "    guid as id,\n" +       // 表达式字段(逻辑字段)
                "    big_age as age + 10,\n" +      // 表达式字段(逻辑字段)
                "    offs bigint metadata from 'offset',\n" +   // 元数据字段
                "    ts timestamp_ltz(3) metadata from 'timestamp'\n" + // 元数据字段
//                "    primary key(id, name) not enforced\n" +
                ")\n" +
                "WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'doit30-4',                            \n" +
                " 'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                " 'properties.group.id' = 'g1',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")");

        tenv.executeSql("desc t_person").print();
        tenv.executeSql("select * from t_person where id > 2").print();

    }
}
