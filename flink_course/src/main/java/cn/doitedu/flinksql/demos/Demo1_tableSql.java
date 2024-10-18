package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo1_tableSql {
    public static void main(String[] args) {
        //TODO FlinkSql
        //     纯sql模式来书写代码

        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode(); // 流计算模式
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);
        //把kafka中的一个topic: doit30-2数据, 映射成一张flinkSql表
        //json: {"id":1, "name":"zs", "age": 28, "gender":"male"}
/*
{"id":1, "name":"zs", "age": 28, "gender":"male"}
{"id":1, "name":"xh", "age": 30, "gender":"female"}
{"id":1, "name":"lisi", "age": 20, "gender":"female"}
{"id":1, "name":"xq", "age": 32, "gender":"male"}
 */
        //create table_x (id int, name string, age int, gender string)
        tableEnv.executeSql("create table t_kafka (\n" +
                "id int,\n" +
                "name string,\n" +
                "age int,\n" +
                "gender string\n" +
                ")\n" +
                "with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'doit30-3',\n" +
                "'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                "'properties.group.id' = 'g1',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'true'\n" +
                ")");

        //把Sql表名, 转换成 table对象
        Table table = tableEnv.from("t_kafka");
        //利用table api进行查询计算
//        table.groupBy($("gender"))
//                .select($("gender"), $("age").avg())
//                .execute()
//                .print();

        tableEnv.executeSql("select gender, avg(age) as avg_age from t_kafka group by gender").print();



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

+----+--------------------------------+-------------+
| op |                         gender |      EXPR$0 |
+----+--------------------------------+-------------+
| +I |                           male |          28 |
| +I |                         female |          30 |
| -U |                         female |          30 |
| +U |                         female |          25 |
| -U |                           male |          28 |
| +U |                           male |          30 |
 */