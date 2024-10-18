package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Watermark 在DDL中的定义示例代码
 * 测试数据：
 *     {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e05","eventTime":1655017436000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e06","eventTime":1655017437000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e07","eventTime":1655017438000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e08","eventTime":1655017439000,"pageId":"p001"}
 *
 *     TODO flink sql watermark 不会像flink Stream一样去 - 1ms
 */
public class Demo9_eventTimeAndWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);
        /**
         * 只有timestamp 或者 timestamp_ltz 类型的字段可以被声明为rowtime（事件时间属性）
         *  + " create table t_events (                                             "
         *  + "      guid int,                                                      "
         *  + "      eventId string,                                                "
         *  + "      eventTime bigint,                                          "
         *  + "      pageId string,                                                 "
         *  + "      pt as proctime(),                                      "
         *  + "      rt as to_timestamp_ltz(eventTime, 3),                             "
         *  + "      watermark for rt as rt - interval '0.001' second                  "
         *  + " ) with (                                                            "
         *  + "      'connector' = 'kafka',                                             "
         *  + "      'topic' = 'doit30-events',                                 "
         *  + "      'properties.bootstrap.servers' = '192.168.157.102:9092',   "
         *  + "      'properties.group.id' = 'g1',                                 "
         *  + "      'scan.startup.mode' = 'earliest-offset',                      "
         *  + "      'format' = 'json',                                            "
         *  + "      'json.fail-on-missing-field' = 'false',                    "
         *  + "      'json.ignore-parse-errors' = 'true'                           "
         *  + " )                                                                           "
         */
        tenv.executeSql(
                " create table t_events (                                             "
                        + "      guid int,                                                      "
                        + "      eventId string,                                                "
                        + "      eventTime bigint,                                          "
                        + "      pageId string,                                                 "
                        + "      pt as proctime(),       " // 利用一个表达式字段, 来声明processing time属性
                        + "      rt as to_timestamp_ltz(eventTime, 3),                             "
                        + "      watermark for rt as rt - interval '0.001' second "
//                        + "      watermark for rt as rt "
                        // 用watermark for xxx,来将一个已经定义的timestamp / timestamp_ltz字段声明成eventTime属性及指定watermark策略
                        + " ) with (                                                            "
                        + "      'connector' = 'kafka',                                             "
                        + "      'topic' = 'doit30-events',                                 "
                        + "      'properties.bootstrap.servers' = '192.168.157.102:9092',   "
                        + "      'properties.group.id' = 'g1',                                 "
                        + "      'scan.startup.mode' = 'earliest-offset',                      "
                        + "      'format' = 'json',                                            "
                        + "      'json.fail-on-missing-field' = 'false',                    "
                        + "      'json.ignore-parse-errors' = 'true'                           "
                        + " )                                                                           ");
/*
+-----------+-----------------------------+-------+-----+-------------------------------------+--------------------------------+
|      name |                        type |  null | key |                              extras |                      watermark |
+-----------+-----------------------------+-------+-----+-------------------------------------+--------------------------------+
|      guid |                         INT |  true |     |                                     |                                |
|   eventId |                      STRING |  true |     |                                     |                                |
| eventTime |                      BIGINT |  true |     |                                     |                                |
|    pageId |                      STRING |  true |     |                                     |                                |
|        pt | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |                       AS PROCTIME() |                                |
|        rt |  TIMESTAMP_LTZ(3) *ROWTIME* |  true |     | AS TO_TIMESTAMP_LTZ(`eventTime`, 3) | `rt` - INTERVAL '0.001' SECOND |
+-----------+-----------------------------+-------+-----+-------------------------------------+--------------------------------+
 */
//        tenv.executeSql("desc t_events").print();
        tenv.executeSql("select guid, eventId, eventTime, pageId, pt, rt, current_watermark(rt) from t_events ").print();

    }
}
