package cn.doitedu.flink.java.demos;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class _19_WaterMark_Api_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * watermark
         */
//        WatermarkStrategy.noWatermarks(); 处理时间,没有watermark
//        WatermarkStrategy.forMonotonousTimestamps(); 事件时间, 紧跟最大事件时间生成watermark
//        WatermarkStrategy.forBoundedOutOfOrderness(); 事件时间, 允许乱序的watermark
//        WatermarkStrategy.forGenerator(); 自定义watermark生成策略
/*
当前数据 -> EventBean(guid=1, eventId=click, timestamp=1000, pageId=101)
当前处理时间 -> 1726848734851
当前watermark -> -9223372036854775808
当前数据 -> EventBean(guid=2, eventId=click, timestamp=3000, pageId=301)
当前处理时间 -> 1726848816177
当前watermark -> -9223372036854775808
当前数据 -> EventBean(guid=3, eventId=click, timestamp=4000, pageId=401)
当前处理时间 -> 1726848926831
当前watermark -> -1001
当前数据 -> EventBean(guid=4, eventId=click, timestamp=5000, pageId=501)
当前处理时间 -> 1726848949866
当前watermark -> 999
 */

        DataStream<String> s1 = env.socketTextStream("192.168.157.102", 8888);

        /**
         * 示例一: 从最源头算子开始, 生成watermark
         */
        // 1. 构造一个watermark的生成策略对象(算法策略, 及事件时间的抽取方法)
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        return Long.parseLong(s.split(",")[2]);
                    }
                });
        // 2. 将构造好的watermark 策略对象, 分配给流(source对象)
//        s1.assignTimestampsAndWatermarks(watermarkStrategy);

        /**
         * 示例二: 不从最源头算子开始生成watermark,而是从中间环节的某个算子开始生成watermark
         * 注意: 如果在源头就已经生成了watermark, 就不要在下游再次产生watermark
         */
        s1.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        }).returns(EventBean.class).setParallelism(2)
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean eventBean, long l) {
                                return eventBean.getTimestamp();
                            }
                        })
        ).process(new ProcessFunction<EventBean, EventBean>() {
                    @Override
                    public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {
                        Thread.sleep(1000);
                        long currentProcessingTime = context.timerService().currentProcessingTime();
                        long currentWatermark = context.timerService().currentWatermark();
                        System.out.println("当前数据 -> " + eventBean);
                        System.out.println("当前处理时间 -> " + currentProcessingTime);
                        System.out.println("当前watermark -> " + currentWatermark);
                    }
                }).setParallelism(1).print()
        ;


        env.execute("watermark execute");
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class EventBean {
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;
}
