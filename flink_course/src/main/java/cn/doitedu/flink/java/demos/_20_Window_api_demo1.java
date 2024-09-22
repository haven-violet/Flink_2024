package cn.doitedu.flink.java.demos;


import cn.doitedu.flink.java.bean.EventBean2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class _20_Window_api_demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        window 分类
            滑动窗口
            滚动窗口
            会话窗口
            计数窗口
            keyed window 和 nonKeyed window
 * 1,e01,10000,p01,10
 * 1,e02,11000,p02,20
 * 1,e02,12000,p03,40
 * 1,e03,20000,p02,10
 * 1,e01,21000,p03,50
 * 1,e04,22000,p04,10
 * 1,e06,28000,p05,60
 * 1,e07,30000,p02,10
         */
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 8888);
        SingleOutputStreamOperator<EventBean2> beanStream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).returns(EventBean2.class);

        //分配watermark, 以推进事件时间
        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                            @Override
                            public long extractTimestamp(EventBean2 eventBean2, long recordTimestamp) {
                                return eventBean2.getTimestamp();
                            }
                        })
        );

        /**
         * 滚动聚合api使用示例
         * 需求一: 每隔10s, 统计最近30s的数据中, 每个用户行为事件条数
         * 使用aggregate算子来实现
         * 这个算子是增量聚合
         */
//        watermarkedBeanStream
//                .keyBy(s -> s.getGuid())
//                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))) // 窗口长度, 滑动步长
//                .aggregate(new AggregateFunction<EventBean2, Integer, Integer>() {
//                    /**
//                     * 初始化累加器
//                     * @return
//                     */
//                    @Override
//                    public Integer createAccumulator() {
//                        return 0;
//                    }
//
//                    /**
//                     * 滚动聚合逻辑(拿到一条数据，如何去更新累加器)
//                     * @param value The value to add
//                     * @param accumulator The accumulator to add the value to
//                     * @return
//                     */
//                    @Override
//                    public Integer add(EventBean2 value, Integer accumulator) {
//                        return accumulator + 1;
//                    }
//
//                    /**
//                     * 从累加器中计算出最终要输出的结果
//                     * @param accumulator The accumulator of the aggregation
//                     * @return
//                     */
//                    @Override
//                    public Integer getResult(Integer accumulator) {
//                        return accumulator;
//                    }
//
//                    /**
//                     * 批计算模式下, 可能需要将多个上游的局部聚合累加器, 放在下游进行全局聚合
//                     * 因为需要对两个累加器进行合并
//                     * 这里就是合并的逻辑
//                     * 流计算模式下, 不用实现
//                     * @param a An accumulator to merge
//                     * @param b Another accumulator to merge
//                     * @return
//                     */
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        return a + b;
//                    }
//                }).print();
        /*
timestamp - (timestamp - offset + windowSize) % windowSize;  windowSize  => slide [SlidingEventTimeWindows]
10000 - (10000 - 0 + 10000) % 10000 = 20000 % 10000

这是滑动窗口 SlidingEventTimeWindows属性

  for (long start = lastStart; start > timestamp - size; start -= slide) {
                windows.add(new TimeWindow(start, start + size));
  }
  1、 start = 10000, lastStart = 10000; timestamp - size = 10000 - 30000 = -20000;
            10000, 10000 + 30000 = 40000
  2、 start = 10000 - 10000  = 0
              0  , 30000
  3、 start = 0 - 10000 = -10000
              -10000, 20000 一下子算了3个窗口
  这一条数据会属于 3个窗口
  1,e01,10000,p01,10
  [10000, 40000)
  [0, 30000)
  [-10000, 20000)

         */


        /*
        需求二: 每隔10s, 统计最近30s的数据中, 每个用户的平均每次行为时长
        要求用aggregate 算子来做聚合
        滚动聚合api使用示例
         */
        watermarkedBeanStream.keyBy(s -> s.getGuid())
            .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
            .aggregate(new AggregateFunction<EventBean2, Tuple2<Integer, Integer>, Double>() {

                /**
                 * 创建累计器
                 * @return tuple2{总时长,总次数}
                 */
                @Override
                public Tuple2<Integer, Integer> createAccumulator() {
                    return Tuple2.of(0, 0);
                }

                /**
                 * 如何更新累加器
                 * @param value The value to add
                 * @param accumulator The accumulator to add the value to
                 * @return
                 */
                @Override
                public Tuple2<Integer, Integer> add(EventBean2 value, Tuple2<Integer, Integer> accumulator) {
                    accumulator.f0 = accumulator.f0 + value.getActTimeLog();
                    accumulator.f1 = accumulator.f1 + 1;
                    return accumulator;
                }


                @Override
                public Double getResult(Tuple2<Integer, Integer> accumulator) {
                    return accumulator.f0 / Double.valueOf(accumulator.f1);
                }

                @Override
                public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {

                    return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                }
            });
//                .print();

        /**
         * 需求三: 每隔10s,统计最近30s的数据中,每个用户的行为事件条数
         * 滚动聚合api使用示例
         * 使用sum算子来实现
         */
        watermarkedBeanStream
                .map(bean -> Tuple2.of(bean, 1)).returns(new TypeHint<Tuple2<EventBean2, Integer>>(){})
                .keyBy(t -> t.f0.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);
//                .print();


        /**
         * 需求四: 每隔10s, 统计最近30s的数据中,每个用户的最大行为时长
         * 滚动聚合api使用示例
         * 用max算子来实现
         * 只有选择中那个字段是max, 其他字段还是首次出现的
         */
        watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .max("actTimeLog");
                //.print();

        /**
         * 需求五: 每隔10s, 统计最近30s的数据中，每个用户的最大行为时长及其所在的那条行为记录
         * 滚动聚合api使用示例
         * 用maxBy算子来实现
         * 会把最大那条数据全部都拿取过来
         */
        watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .maxBy("actTimeLog");
        //        .print();

        /**
         * 需求六: 每隔10s, 统计最近30s的数据中，每个页面上发生的行为中，平均时长最大的前2种事件及其平均时长
         */
        watermarkedBeanStream
                .keyBy(EventBean2::getPageId)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean2, Tuple3<String, String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<EventBean2, Tuple3<String, String, Double>, String, TimeWindow>.Context context, Iterable<EventBean2> elements, Collector<Tuple3<String, String, Double>> out) throws Exception {
//                        context
//                        context.window().
//                        getRuntimeContext().
                        HashMap<String, Tuple2<Integer, Integer>> map = new HashMap<>();
                        for (EventBean2 element : elements) {
//                            if(map.containsKey(element.getEventId())) {
//                                Tuple2<Integer, Integer> curActTimeAndCount = map.get(element.getEventId());
//                                curActTimeAndCount.f0 = curActTimeAndCount.f0 + element.getActTimeLog();
//                                curActTimeAndCount.f1 = curActTimeAndCount.f1 + 1;
//                            }else {
//                                map.put(element.getEventId(), Tuple2.of(element.getActTimeLog(), 1));
//                            }
                            Tuple2<Integer, Integer> curActTimeAndCount = map.getOrDefault(element.getEventId(), Tuple2.of(0, 0));
                            map.put(element.getEventId(), Tuple2.of(curActTimeAndCount.f0 + element.getActTimeLog(), curActTimeAndCount.f1 + 1));
                        }
                        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
                        for (Map.Entry<String, Tuple2<Integer, Integer>> entry : map.entrySet()) {
                            list.add(Tuple2.of(entry.getKey(), entry.getValue().f0 / Double.valueOf(entry.getValue().f1)));
                        }
                        Collections.sort(list, new Comparator<Tuple2<String, Double>>() {
                            @Override
                            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                                return Double.compare(o2.f1, o1.f1);
                            }
                        });
                        //比较完成输出前2个
                        for (int i = 0; i < Math.min(list.size(), 2); i++) {
                            out.collect(Tuple3.of(key, list.get(i).f0, list.get(i).f1));
                        }
                    }
                });
                //.print();


        /**
         * 需求七: 每隔10s,统计最近30s的数据中,每个用户的行为事件中,行为时长最长的前2条记录
         * 使用apply实现
         */
        watermarkedBeanStream.keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .apply(new WindowFunction<EventBean2, EventBean2, Long, TimeWindow>() {

                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<EventBean2> elements, Collector<EventBean2> out) throws Exception {
//                        getRun 相比较于process() 没有上下文对象了
//                        window 相比较于process() 只是context的一个属性，能够获取的信息确实更加欠缺
                        ArrayList<Tuple2<EventBean2, Integer>> list = new ArrayList<>();
                        for (EventBean2 element : elements) {
                            list.add(Tuple2.of(element, element.getActTimeLog()));
                        }
                        Collections.sort(list, new Comparator<Tuple2<EventBean2, Integer>>() {
                            @Override
                            public int compare(Tuple2<EventBean2, Integer> o1, Tuple2<EventBean2, Integer> o2) {
                                return Integer.compare(o2.f1, o1.f1);
                            }
                        });
                        for (int i = 0; i < Math.min(list.size(), 2); i++) {
                            out.collect(list.get(i).f0);
                        }
                    }
                }).print();



        env.execute("window demo");
    }
}
