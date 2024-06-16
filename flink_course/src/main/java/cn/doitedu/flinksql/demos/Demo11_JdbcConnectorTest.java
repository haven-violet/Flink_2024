package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Demo11_JdbcConnectorTest {
    public static void main(String[] args) {
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);
        tableEnv.executeSql("create table flink_stu(\n" +
                "        id int, \n" +
                "        name string,\n" +
                "        age int,\n" +
                "        gender string\n" +
                ") with(\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest?useSSL=false',\n" +
                "'table-name' = 'stu',\n" +
                "'username' = 'root',\n" +
                "'password' = 'root'        \n" +
                ")");

        tableEnv.executeSql("select * from flink_stu").print();



    }
}

