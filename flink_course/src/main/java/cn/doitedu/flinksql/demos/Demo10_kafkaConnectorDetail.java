package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo10_kafkaConnectorDetail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSettings);
        /**
         * 对应kafka中的数据
         *  key: {"k1": 100, "k2": 200}
         *  value: {"guid":1, "eventId":"e02", "eventTime":1655017433000, "pageId": "p001"}
         *  headers:
         *      h1 -> vvvv
         *      h2 -> tttt
         *
         *      +"create table t_kafka_connector ( "
         *      +"    guid int, "
         *      +"    eventId string, "
         *      +"    eventTime bigint, "
         *      +"    pageId string, "
         *      +"    k1 int, "
         *      +"    k2 int, "
         *      +"    rec_ts timestamp(3) metadata from 'timestamp', "
         *      +"    `offset` bigint metadata, "
         *      +"    headers map<string, bytes> metadata, "
         *      +"    rt as to_timestamp_ltz(eventTime, 3), "
         *      +"    watermark for rt as rt - interval '0.001' second "
         *      +") with ( "
         *      +"    'connector' = 'kafka', "
         *      +"    'topic' = 'doit30-kafka', "
         *      +"    'properties.bootstrap.servers' = '192.168.157.102:9092', "
         *      +"    'properties.group.id' = 'testGroup', "
         *      +"    'scan.startup.mode' = 'earliest-offset', "
         *      +"    'key.format' = 'json', "
         *      +"    'key.json.ignore-parse-errors' = 'true', "
         *      +"    'key.fields' = 'k1,k2', "
         *      +"    // /**'key.fields-prefix' "
         *      +"    'value.format' = 'json', "
         *      +"    'value.json.fail-on-missing-field' = 'false', "
         *      +"    'value.fields-include' = 'EXCEPT_KEY' "
         *      +") "
         */
        tenv.executeSql(
               "create table t_kafka_connector ( "
                        +"    guid int, "
                        +"    eventId string, "
                        +"    eventTime bigint, "
                        +"    pageId string, "
                        +"    k1 int, "
                        +"    k2 int, "
                        +"    rec_ts timestamp(3) metadata from 'timestamp', "
                        +"    `offset` bigint metadata, "
                        +"    headers map<string, bytes> metadata, "
                        +"    rt as to_timestamp_ltz(eventTime, 3), "
                        +"    watermark for rt as rt - interval '0.001' second "
                        +") with ( "
                        +"    'connector' = 'kafka', "
                        +"    'topic' = 'doit30-kafka', "
                        +"    'properties.bootstrap.servers' = '192.168.157.102:9092', "
                        +"    'properties.group.id' = 'testGroup', "
                        +"    'scan.startup.mode' = 'earliest-offset', "
                        +"    'key.format' = 'json', "
                        +"    'key.json.ignore-parse-errors' = 'true', "
                        +"    'key.fields' = 'k1;k2', "
                       /* +"    // /**'key.fields-prefix' " */
                        +"    'value.format' = 'json', "
                        +"    'value.json.fail-on-missing-field' = 'false', "
                        +"    'value.fields-include' = 'EXCEPT_KEY' "
                        +") ");

//        tenv.executeSql("select * from t_kafka_connector").print();
        tenv.executeSql("select guid, eventId, cast ( headers['h1'] as string) as h1, cast(headers['h2'] as string ) as h2 from t_kafka_connector").print();
    }
}
