package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Demo5_CatalogDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 环境创建之初, 底层会自动初始化一个元数据空间实现对象(default_catalog => genericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //创建了一个hive元数据空间的实现对象
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "D:\\workspace\\idea\\Flink_2024\\conf\\hiveconf");

        //将hive元数据空间对象注册到环境中
        tenv.registerCatalog("mycatalog", hiveCatalog);
        tenv.executeSql("create temporary table `mycatalog`.`default`.`t_kafka` (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    age int,\n" +
                "    gender string\n" +
                ") with (\n" +
                "'connector' = 'kafka',                             \n" +
                "'topic' = 'doit30',                              \n" +
                "'properties.bootstrap.servers' = '192.168.157.102:9092',   \n" +
                "'properties.group.id' = 'g1',                      \n" +
                "'scan.startup.mode' = 'earliest-offset',           \n" +
                "'format' = 'json',                                 \n" +
                "'json.fail-on-missing-field' = 'false',            \n" +
                "'json.ignore-parse-errors' = 'true'    \n" +
                ")");

        tenv.executeSql("create temporary table `t_kafka2` (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    age int,\n" +
                "    gender string\n" +
                ") with (\n" +
                "'connector' = 'kafka',                             \n" +
                "'topic' = 'doit30',                              \n" +
                "'properties.bootstrap.servers' = '192.168.157.102:9092',   \n" +
                "'properties.group.id' = 'g2',                      \n" +
                "'scan.startup.mode' = 'earliest-offset',           \n" +
                "'format' = 'json',                                 \n" +
                "'json.fail-on-missing-field' = 'false',            \n" +
                "'json.ignore-parse-errors' = 'true'    \n" +
                ")");
        tenv.executeSql("create view if not exists `mycatalog`.`default`.`t_kafka_view` \n" +
                "as \n" +
                "select id, name, age from `mycatalog`.`default`.`t_kafka`");

        //列出当前会话中所有的catalog
        tenv.listCatalogs();
        System.out.println("---1----");
        //列出default_catalog中的库和表
        tenv.executeSql("show catalogs").print();
        tenv.executeSql("use catalog default_catalog").print();
        tenv.executeSql("show databases").print();
        tenv.executeSql("use default_database");
        tenv.executeSql("show tables").print();

        System.out.println("---2-----");
        //列出mycatalog中的库和表
        tenv.executeSql("use catalog mycatalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use `default`");
        tenv.executeSql("show tables").print();

        System.out.println("--------");
        //列出临时表
        tenv.listTemporaryTables();

    }
}
/*

create view if not exists `mycatalog`.`default`.`t_kafka_view`
as
select id, name, age from `mycatalog`.`default`.`t_kafka`

tenv.executeSql("create  view if not exists `mycatalog`.`default`.`t_kafka_view` as select id,name,age from `mycatalog`.`default`.`t_kafka`");



create temporary table `mycatalog`.`default`.`t_kafka` (
    id int,
    name string,
    age int,
    gender string
) with (
'connector' = 'kafka',
'topic' = '192.168.157.102',
'properties.bootstrap.servers' = '192.168.157.102:9092',
'properties.group.id' = 'g1',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json',
'json.fail-on-missing-field' = 'false',
'json.ignore-parse-errors' = 'true'
)
 */
