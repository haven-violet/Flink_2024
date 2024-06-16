package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Demo12_JdbcConnectorTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);


        tenv.executeSql("create table flink_stu(\n" +
                "gender string primary key,\n" +
                "avg_age int,\n" +
                "sum_age int \n" +
                ") with (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest?useSSL=false',\n" +
                "'table-name' = 'stu2',\n" +
                "'username' = 'root',\n" +
                "'password' = 'root'\n" +
                ")");

        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<User> user = s1.map(s -> {
            String[] split = s.split(",");
            return new User(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
        }).returns(User.class);

        DataStreamSource<String> s2 = env.socketTextStream("192.168.157.102", 9997);
        SingleOutputStreamOperator<User> user2 = s1.map(s -> {
            String[] split = s.split(",");
            return new User(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
        }).returns(User.class);

        //流转表
        tenv.createTemporaryView("user1", user);


        tenv.executeSql(" insert into flink_stu(gender, avg_age) select gender, avg(age)   from user1 group by gender");

        tenv.executeSql(" insert into flink_stu(gender, sum_age) select gender, sum(age)   from user1 group by gender");


        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User {
        private int id;
        private String name;
        private String gender;
        private int age;
    }
}
