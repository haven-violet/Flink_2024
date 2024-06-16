package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Demo1_TableSql {
    public static void main(String[] args) {
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);
        // 把kafka中的一个topic:  flink-sql-1 数据映射成一个flinkSql表
        // json: {"id":1, "name":"zs", "age":28, "gender":"male"}
        // create table_x(id int, name String, age int, gender string)
        tableEnv.executeSql("create table t_kafka (\n" +
                " id int,\n" +
                " name string,\n" +
                " age int,\n" +
                " gender string\n" +
                ")\n" +
                "with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'flink-sql-1',\n" +
                "'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "'properties.group.id' = 'g1',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'true'\n" +
                ")");

        tableEnv.executeSql("select gender, avg(age) as avg_age from t_kafka group by gender").print();

    }
}
