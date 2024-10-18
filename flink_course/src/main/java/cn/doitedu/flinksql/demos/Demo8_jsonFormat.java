package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo8_jsonFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);

        /**
         * 一、简单嵌套json格式,建表示例
         *  嵌套对象解析成Map类型
         *  {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         *  +"create table t_json1 (                "
         *  +"    id int,                           "
         *  +"    name map<string, string>,         "
         *  +"    bigid as id * 10                  "
         *  +") with (                              "
         *  +"    'connector' = 'filesystem',       "
         *  +"    'path' = 'data/json/qiantao'      "
         *  +"    'format' = 'json'                 "
         *  +")                                     "
         */
//        tenv.executeSql(
//                "create table t_json1 (               "
//                        +"    id int,                           "
//                        +"    name map<string, string>,         "
//                        +"    bigid as id * 10                  "
//                        +") with (                              "
//                        +"    'connector' = 'filesystem',       "
//                        +"    'path' = 'data/json/qiantao',      "
//                        +"    'format' = 'json'                 "
//                        +")                                     ");

//        tenv.executeSql("desc t_json1");
//        tenv.executeSql(" select * from t_json1 ").print();
//        tenv.executeSql("select id, name['nick'] as nick from t_json1 ").print();

        /**
         * 二、简单嵌套json建表示例
         * 嵌套对象, 解析成Row类型
         * {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         * id int,
         * name row<nick string, formal string, height int>
         *
         *
         */
        tenv.createTable("t_json2",
                TableDescriptor
                        .forConnector("filesystem")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.ROW(
                                                DataTypes.FIELD("nick", DataTypes.STRING()),
                                                DataTypes.FIELD("formal", DataTypes.STRING()),
                                                DataTypes.FIELD("height", DataTypes.INT())
                                        ))
                                        .build()
                        )
                        .format("json")
                        .option("path", "data/json/qiantao")
                        .build());

//        tenv.executeSql("desc t_json2").print();
/*
+------+---------------------------------------------------+------+-----+--------+-----------+
| name |                                              type | null | key | extras | watermark |
+------+---------------------------------------------------+------+-----+--------+-----------+
|   id |                                               INT | true |     |        |           |
| name | ROW<`nick` STRING, `formal` STRING, `height` INT> | true |     |        |           |
+------+---------------------------------------------------+------+-----+--------+-----------+
 */
        //查询每个人的id和 formal和height
            //tenv.executeSql( "select id, name.formal, name.height from t_json2" ).print();

        /**
         * 三、复杂嵌套json, 建表示例
         * {"id":1,"friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
         *
         * + "create table t_json3 (                                                                        "
         * + "     id int,                                                                                   "
         * + "     friends array<row<name string, info map<string, string>>>                            "
         * + ") with (                                                                              "
         * + "     'connector' = 'filesystem',                                                              "
         * + "     'path' = 'data/json/qiantao3/',                                                      "
         * + "     'format' = 'json'                                                                         "
         * + ")                                                                                              "
         */
        tenv.executeSql(
                "create table t_json3 (                                                                        "
                        + "     id int,                                                                                   "
                        + "     friends array<row<name string, info map<string, string>>>                            "
                        + ") with (                                                                              "
                        + "     'connector' = 'filesystem',                                                              "
                        + "     'path' = 'data/json/qiantao3/',                                                      "
                        + "     'format' = 'json'                                                                         "
                        + ")                                                                                              "   );
        //tenv.executeSql("desc t_json3").print();
/*
+---------+-------------------------------------------------------+------+-----+--------+-----------+
|    name |                                                  type | null | key | extras | watermark |
+---------+-------------------------------------------------------+------+-----+--------+-----------+
|      id |                                                   INT | true |     |        |           |
| friends | ARRAY<ROW<`name` STRING, `info` MAP<STRING, STRING>>> | true |     |        |           |
+---------+-------------------------------------------------------+------+-----+--------+-----------+
 */
        //tenv.executeSql("select * from t_json3 ").print();
        /*
        select id,
               friends[1].name as name1, friends[1].info['addr'] as addr1, friends[1].info['gender'] as gender1,
               friends[2].name as name2, friends[2].info['addr'] as addr2, friends[2].info['gender'] as gender2
        from t_json3
         */
        tenv.executeSql("        select id,\n" +
                "               friends[1].name as name1, friends[1].info['addr'] as addr1, friends[1].info['gender'] as gender1, \n" +
                "               friends[2].name as name2, friends[2].info['addr'] as addr2, friends[2].info['gender'] as gender2\n" +
                "        from t_json3").print();






    }
}
