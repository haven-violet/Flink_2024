package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO Flink 测试 mysql-cdc connector连接器效果
 */
public class Demo14_mysqlCdcConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting );
/*
create table flink_score (
    id int,
    name string,
    gender string,
    score double，
    primary key(id) not enforced
) with (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.157.102',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root',
    'database-name' = 'flinktest',
    'table-name' = 'score'
)

create table t1 (
    id int,
    name string,
    primary key(id) not enforced
) with (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.157.102',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root',
    'database-name' = 'flinktest',
    'table-name' = 't1'
)

 */     env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/checkpoint");
        tenv.executeSql("create table flink_score (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    gender string,\n" +
                "    score double, \n" +
                "    primary key(id) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '192.168.157.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'database-name' = 'flinktest',\n" +
                "    'table-name' = 'score',\n" +
                "    'jdbc.properties.useSSL' = 'false'\n" +
                ")");
        tenv.executeSql("\n" +
                "create table t1 (\n" +
                "    id int, \n" +
                "    name string, \n" +
                "    primary key(id) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '192.168.157.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'database-name' = 'flinktest',\n" +
                "    'table-name' = 't1',\n" +
                "    'jdbc.properties.useSSL' = 'false'\n" +
                ")");
        //tenv.executeSql("select * from t1").print();
        //tenv.executeSql("select * from flink_score").print();

        //tenv.executeSql("select gender, avg(score) as avg_score from flink_score group by gender").print();

        /*
        create table flink_rank (
            gender string,
            name string,
            score_amt double,
            rn bigint,
            primary key(gender, rn) not enforced
        ) with (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest',
            'table_name' = 'score_rank',
            'username' = 'root',
            'password' = 'root'
        )

        insert into flink_rank
        select
            gender,
            name,
            score_amt,
            rn
        from (
            select
                gender,
                name,
                score_amt,
                row_number() over(partition by gender order by score_amt desc) as rn
            from (
                select
                    gender, name,
                    sum(score) as score_amt
                from flink_score
                group by gender, name
            ) a
        ) b
        where rn <= 2
         */
        //TODO 建一个目标表, 用来存放查询结果: 每种性别中, 总分最高的前2个人
        tenv.executeSql("create table flink_rank1 (\n" +
                "    gender string, \n" +
                "    name string, \n" +
                "    score_amt double,\n" +
                "    rn bigint,\n" +
                "    primary key(gender, rn) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.157.102:3306/flinktest?useSSL=false&characterEncoding=UTF-8&userSSL',\n" +
                "    'table-name' = 'flink_rank',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ")");

        tenv.executeSql(
                "           insert into flink_rank1 \n" +
                "        select \n" +
                "            gender, \n" +
                "            name, \n" +
                "            score_amt,\n" +
                "            rn\n" +
                "        from (\n" +
                "            select\n" +
                "                gender,\n" +
                "                name,\n" +
                "                score_amt,\n" +
                "                row_number() over(partition by gender order by score_amt desc) as rn\n" +
                "            from (\n" +
                "                select\n" +
                "                    gender, name,\n" +
                "                    sum(score) as score_amt\n" +
                "                from flink_score\n" +
                "                group by gender, name\n" +
                "            ) a\n" +
                "        ) b \n" +
                "        where rn <= 2");



    }
}
