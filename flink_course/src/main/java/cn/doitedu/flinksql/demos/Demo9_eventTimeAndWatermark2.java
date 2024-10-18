package cn.doitedu.flinksql.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * TODO
 *  流 -> 表, 过程中如何传承 事件时间 和 watermark
 *  除了自己显示指定, 否则 流 -> 表过程中 watermark会丢失
 *  流的并行度会挑选最小watermark,所以设置成1个并行度较好观察
 */
public class Demo9_eventTimeAndWatermark2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSettings);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Event> s2 = s1.map(s -> JSON.parseObject(s, Event.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getEventTime();
                            }
                        }));
        // 观察流上的watermark推进
//        s2.process(new ProcessFunction<Event, String>() {
//            @Override
//            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
//                long wm = ctx.timerService().currentWatermark();
//                out.collect(value + " => " + wm);
//            }
//        }).print();

        //这样，直接把流 转成 表, 会丢失 watermark
        tenv.createTemporaryView("t_events", s2);
//        tenv.executeSql("select guid, eventId, eventTime, pageId, current_watermark(eventTime) from t_events ").print();

        //测试验证watermark的丢失, 流 -》表 -》流 watermark丢失了
        Table table = tenv.from("t_events");
        DataStream<Row> ds = tenv.toDataStream(table);
        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(row + " => " + ctx.timerService().currentWatermark());
            }
        })/*.print()*/;

        //可以在流 -》表时，显示声明watermark策略
        tenv.createTemporaryView("t_events2", s2,
                Schema.newBuilder()
                        .column("guid", DataTypes.INT())
                        .column("eventId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .column("pageId", DataTypes.STRING())
//                        //重新利用一个bigint转成timestamp后,作为事件时间属性
//                        .columnByExpression("rt", "to_timestamp_ltz(eventTime,3)")
//                        //重新定义表上的watermark策略
//                        .watermark("rt", "rt - interval '1' second")
                        //利用底层流连接器暴露的rowtime元数据 （代表的是底层流中每条数据上的eventTime）,声明成事件时间属性字段
                        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                        .watermark("rt", "source_watermark()")
                        .build());

        tenv.executeSql("select guid, eventId, eventTime, pageId, rt, current_watermark(rt) as wm from t_events2 ").print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        private int guid;
        private String eventId;
        private long eventTime;
        private String pageId;

    }
}
