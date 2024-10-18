package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo12_JdbcConnectorTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);
        /*
        create table
         */
        SingleOutputStreamOperator<Bean1> bean1 = env.socketTextStream("192.168.157.102", 9998).map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });

        SingleOutputStreamOperator<Bean2> bean2 = env.socketTextStream("192.168.157.102", 9999).map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });

        //流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);

        //创建 mysql-cdc 连接的表，往里面插入changelog流数据
        /*

        create table flink_stu (
            id int primary,
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
        /**
         * TODO
         *      jdbc connector
         *          读:  只能一次性读取数据就完事了,后面新增修改的数据的话，不会再去读取数据更新数据
         *          写:  支持changelog流, 能够支持 +i, -u, +u, -d 的操作针对表来说
         *          情况一:
         *  CREATE TABLE `stu3` (
         *   `in_id` int(11) NOT NULL AUTO_INCREMENT,
         *   `id` int(11) DEFAULT NULL,
         *   `name` varchar(255) DEFAULT NULL,
         *   `gender` varchar(255) DEFAULT NULL,
         *   PRIMARY KEY (`in_id`)
         *   ) ENGINE=InnoDB DEFAULT CHARSET=utf8;    >>> 存储下了2条数据, +i, -d, +i
         *  1	1		male
         *  2	1	li	male
         *          情况二:
         *  CREATE TABLE `stu3` (
         *   `in_id` int(11) NOT NULL AUTO_INCREMENT,
         *   `id` int(11) DEFAULT NULL,
         *   `name` varchar(255) DEFAULT NULL,
         *   `gender` varchar(255) DEFAULT NULL,
         *   PRIMARY KEY (`in_id`),
         *   UNIQUE KEY `id` (`id`)
         *  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;    >>> 只存储下了1条数据, +i, -d, +i
         *  1	1	li	male
         *
         *
         *
         *
         */
        tenv.executeSql("\n" +
                "        create table flink_stu (\n" +
                "            id int primary key,\n" +
                "            name string,\n" +
                "            gender string\n" +
                "        ) with (\n" +
                "            'connector' = 'jdbc',\n" +
                "            'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest?useSSL=false&characterEncoding=UTF-8&userSSL',\n" +
                "            'table-name' = 'stu3',\n" +
                "            'username' = 'root',\n" +
                "            'password' = 'root'\n" +
                "        )\n" +
                "         ");

        // flink sql  bean1 left join bean2 产生 +i, -u, +u changelog流数据写入mysql中
        tenv.executeSql("insert into flink_stu" +
                " select bean1.id, bean2.name, bean1.gender  from bean1 left join bean2 on bean1.id = bean2.id ");


    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean1 {
        private int id;
        private String gender;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean2 {
        private int id;
        private String name;
    }
}
