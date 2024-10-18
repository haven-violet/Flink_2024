package cn.doitedu.flinksql.demos;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo6_exercise {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
/*
kafka中有如下数据

{"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
{"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
{"id":2,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
{"id":3,"name":"zs","nick":"tn","age":18,"gender":"male"}

现在需要用flinksql来对上述数据进行查询统计
截止到当前,每个昵称,都有多少个用户
截止到当前,每个性别,年龄最大值
 */
        tenv.executeSql("create table t_person (\n" +
                "id int,\n" +
                "name string,\n" +
                "nick string,\n" +
                "age int,\n" +
                "gender string\n" +
                ")\n" +
                "with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'doit30-3',\n" +
                "'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                "'properties.group.id' = 'g1',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'true'\n" +
                ")");

        //建表
        //kafka连接器，不能接受 UPDATE修正模式的数据, 只能接受INSERT模式的数据
        //而我们的查询语句产生的结果，存在update模式, 就需要另一种连接器表(upsert-kafka)来接收
        tenv.executeSql("        create table t_nick_cnt (\n" +
                "            nick string primary key not enforced,\n" +
                "            user_cnt bigint\n" +
                "        )with(\n" +
                "            'connector' = 'upsert-kafka',\n" +
                "            'topic' = 'doit30-nick',\n" +
                "            'properties.bootstrap.servers' = '192.168.157.102:9092',\n" +
                "            'key.format' = 'json',\n" +
                "            'value.format' = 'json'\n" +
                "        )");
        /*
        create table t_nick_cnt (
            nick string primary key not enforced,
            user_cnt bigint
        )with(
            'connector' = 'upsert-kafka',
            'topic' = 'doit30-nick',
            'properties.bootstrap.servers' = '192.168.157.102:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
         */

        tenv.executeSql("insert into t_nick_cnt select nick, count(distinct id) as user_cnt from t_person group by nick").print();

    }
}

