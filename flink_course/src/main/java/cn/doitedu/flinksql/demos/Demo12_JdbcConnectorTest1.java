package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo12_JdbcConnectorTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);
        //建表来映射mysql中的flinktest.stu
        /*
        create table flink_stu (
            id int primary key,
            name string,
            age int,
            gender string
        ) with (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest',
            'table-name' = 'stu',
            'username' = 'root',
            'password' = 'root'
        )
         */
        tenv.executeSql("create table flink_stu (\n" +
                "            id int primary key,\n" +
                "            name string,\n" +
                "            age int,\n" +
                "            gender string\n" +
                "        ) with (\n" +
                "            'connector' = 'jdbc',\n" +
                "            'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest?useSSL=false&characterEncoding=UTF-8&userSSL',\n" +
                "            'table-name' = 'stu',\n" +
                "            'username' = 'root',\n" +
                "            'password' = 'root'\n" +
                "        )");

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 9999);
        tenv.executeSql("select * from flink_stu").print();

        source.print();
        env.execute();
    }
}
