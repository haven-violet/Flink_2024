package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class Demo7_columnDetail_tableApi {

    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        //建表(数据源表)
        //{"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.createTable("t_person",
                TableDescriptor
                        .forConnector("kafka")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT()) // column是声明物理字段到表结构中来
                                        .column("name", DataTypes.STRING()) // column是声明物理字段到表结构中来
                                        .column("nick", DataTypes.STRING()) // column是声明物理字段到表结构中来
                                        .column("age", DataTypes.INT()) // column是声明物理字段到表结构中来
                                        .column("gender", DataTypes.STRING()) // column是声明物理字段到表结构中来
                                        .columnByExpression("guid", "id") // 声明表达式字段到表结构中来
//                                        .columnByExpression("age", $("age").plus(10)) 声明表达式字段到表结构中来
                                        .columnByExpression("big_age", "age + 10") //声明表达式字段
                                        //isVirtual 是表示: 当这个表被sink表时,该字段是否出现在schema中
                                        .columnByMetadata("offs", DataTypes.BIGINT(), "offset", true) //声明元数据字段
                                        .columnByMetadata("ts", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true) //声明元数据字段
                                        //.primaryKey("id", "name")
                                        .build())
                        .format("json")
                        .option("topic", "doit30-4")
                        .option("properties.bootstrap.servers", "192.168.157.102:9092")
                        .option("properties.group.id", "g1")
                        .option("scan.startup.mode", "earliest-offset")
                        .option("json.fail-on-missing-field", "false")
                        .option("json.ignore-parse-errors", "true")
                        .build());
        tenv.executeSql("select * from t_person ").print();
    }
}
