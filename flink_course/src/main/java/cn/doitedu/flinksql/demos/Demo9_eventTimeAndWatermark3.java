package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * TODO
 *      表 -》流 如何传承 事件时间 和 watermark
 */
public class Demo9_eventTimeAndWatermark3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSettings);
        tenv.executeSql(
                " create table t_events (                                                 "
                        + "     guid int,                                                           "
                        + "     eventId string,                                                         "
                        // 利用一个表达式字段，来声明processing time属性
                        + "     eventTime bigint,                                                       "
                        + "     pageId string,                                                      "
                        + "     pt as proctime(),                                                       "
                        + "     rt as to_timestamp_ltz(eventTime, 3),                                   "
                        // 用watermark for xxx, 来将一个已定义的timestamp / timestamp_ltz字段声明成eventTime属性及指定watermark策略
                        + "     watermark for rt as rt - interval '1' second                "
                        + "  ) with (                                                                   "
                        + "     'connector' = 'kafka',                                              "
                        + "     'topic' = 'doit30-events2',                                             "
                        + "     'properties.bootstrap.servers' = '192.168.157.102:9092',                      "
                        + "     'properties.group.id' = 'g1',                                           "
                        + "     'scan.startup.mode' = 'earliest-offset',                                    "
                        + "     'format' = 'json',                                                          "
                        + "     'json.fail-on-missing-field' = 'false',                                     "
                        + "     'json.ignore-parse-errors' = 'true'                                         "
                        + "  )                                                                              ");
        //tenv.executeSql("select guid, eventId, rt, current_watermark(rt) as wm from t_events ").print();

        DataStream<Row> ds = tenv.toDataStream(tenv.from("t_events"));
        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value + " => " + ctx.timerService().currentWatermark());
            }
        }).print();


        env.execute("");
    }
}
