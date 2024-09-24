package cn.doitedu.flink.java.demos;


import cn.doitedu.flink.java.bean.EventBean2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class _21_Window_Api_demo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        /**
         * forBoundedOutOfOrderness() Duration.ofSeconds() 这个小乱序 将watermark推进变慢指定时间
         * allowedLateness  这个中乱序, 允许窗口数据迟到指定时间, 虽然窗口触发过了,但是窗口数据不清除, 来一条触发一次计算
         *          假设是(10, 20] 窗口, 允许迟到2S, 就是 到了22时候, 再来数据的话, 只能进入侧输出流中
         * sideOutputDate  这个大乱序, 就是数据在窗口允许迟到时间限制外, 还来这个窗口的数据, 只能进入测输出流中
         *          然后用户自己决定怎么处理这部分迟到很久的数据
         */

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> beanStream = source.map(s -> {
                    String[] arr = s.split(",");
                    EventBean2 bean = new EventBean2(Long.parseLong(arr[0]), arr[1], Long.parseLong(arr[2]), arr[3], Integer.parseInt(arr[4]));
                    return Tuple2.of(bean, 1);
                }).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<EventBean2, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean2, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<EventBean2, Integer> element, long recordTimestamp) {
                                        return element.f0.getTimestamp();
                                    }
                                })
                );

        OutputTag<Tuple2<EventBean2, Integer>> lateDataOutputTag = new OutputTag<>("late_data", TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {}));

        SingleOutputStreamOperator<String> sumResultStream = beanStream.keyBy(tp -> tp.f0.getGuid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 事件时间滚动窗口, 窗口长度10s   10 - (10 + 10) % 10
                .allowedLateness(Time.seconds(2))  // 允许迟到2s
                .sideOutputLateData(lateDataOutputTag) // 迟到超过允许时限的数据, 输出到该"outputTag"的测流中
                .apply(new WindowFunction<Tuple2<EventBean2, Integer>, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Tuple2<EventBean2, Integer>> input, Collector<String> out) throws Exception {
                        int count = 0;
                        for (Tuple2<EventBean2, Integer> eventBean2IntegerTuple2 : input) {
                            count++;
                        }
                        out.collect(window.getStart() + ":" + window.getEnd() + "," + count);
                    }
                });
        sumResultStream.print("主流结果");
        DataStream<Tuple2<EventBean2, Integer>> lateDataSideStream = sumResultStream.getSideOutput(lateDataOutputTag);
        lateDataSideStream.print("侧流结果");

        env.execute();
/*
 * 1,e01,10000,p01,10
 * 1,e02,11000,p02,20
 * 1,e02,12000,p03,40
 * 1,e03,20000,p02,10
 * 1,e01,21000,p03,50
 * 1,e04,22000,p04,10
 * 1,e06,28000,p05,60
 * 1,e07,30000,p02,10
 *
 * 1,e01,1000,p01,10
 *
 * 1,e03,21000,p02,10
 * 1,e03,21999,p02,10
 * 1,e03,22000,p02,10
 */
    }
}
