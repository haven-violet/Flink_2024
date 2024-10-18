package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo14_streamFromToTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Person> s2 = s1.map(s -> {
            String[] split = s.split(",");
            return new Person(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
        });

        /*
        1,zs,male,18
        2,lisi,male,30
        3,zhaoliu,male,80
        4,tianqi,male,67
         */

        //把流变成表
        // 注册了sql表名, 后续可以用sql语句查询
        tenv.createTemporaryView("abc", s2);
        // 得到table对象, 后续可以用api来查询
        //Table table = tenv.fromDataStream(s2);

        //做查询: 每种性别中年龄最大的3个人信息
        /*
        select
            id,
            name,
            age,
            gender,
            rn
        from (
            select
                id, name, gender, age,
                row_number() over(partition by gender order by age desc) as rn
            from abc
        ) a
        where rn <= 3
         */

        String sql1 = "        select \n" +
                "            id,\n" +
                "            name,\n" +
                "            age,\n" +
                "            gender,\n" +
                "            rn             \n" +
                "        from (\n" +
                "            select \n" +
                "                id, name, gender, age, \n" +
                "                row_number() over(partition by gender order by age desc) as rn\n" +
                "            from abc \n" +
                "        ) a \n" +
                "        where rn <= 3 ";

        /*
        topN的查询结果, 创建为视图, 继续查询
        方式一
         */
        Table tmp = tenv.sqlQuery(sql1);
        tenv.createTemporaryView("tmp", tmp);
        tenv.executeSql("select * from tmp where age % 2 = 1 ");

        /*
        topN的查询结果, 创建为视图, 继续查询
        方式二
         */

        tenv.executeSql("        create temporary view topn_view\n" +
                "        as\n" +
                "        select\n" +
                "            id,\n" +
                "            name,\n" +
                "            age,\n" +
                "            gender,\n" +
                "            rn\n" +
                "        from (\n" +
                "            select\n" +
                "                id, name, gender, age,\n" +
                "                row_number() over(partition by gender order by age desc) as rn\n" +
                "            from abc\n" +
                "        ) a\n" +
                "        where rn <= 3");
        tenv.executeSql(" create temporary view topn_odd as select * from topn_view where age % 2 = 1 ");

        /*
        创建目标 kafka 映射表
        create table t_upsert_kafka2 (
            id int,
            name string,
            age int,
            gender string,
            rn bigint,
            primary key(gender, rn) not enforced
        ) with (
            'connector' = 'upsert-kafka',
            'topic' = 'doit30-topn',
            'properties.bootstrap.servers' = '192.168.157.102:9092',
            'key.format' = 'csv',
            'value.format' = 'csv'
        )
         */



    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person{
        private int id;
        private String name;
        private String gender;
        private int age;
    }
}
