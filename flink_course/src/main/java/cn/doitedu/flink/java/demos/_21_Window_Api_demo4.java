package cn.doitedu.flink.java.demos;


import cn.doitedu.flink.java.bean.EventBean2;
import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

public class _21_Window_Api_demo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        /**
         * TODO
         *   trigger 窗口触发器, 是每来一条数据进行检查或者watermark是否到达指定时间, 就会开始触发计算
         *   evictor 窗口数据移除器, 可以在窗口触发时候，选择在触发前或者窗口计算后进行干些什么事情
         */

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 8888);
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

        /**
         * 每次来一条e0x, 进行一次窗口计算 + 原来本身窗口正常触发计算也要
         */
        SingleOutputStreamOperator<String> sumResultStream = beanStream.keyBy(tp -> tp.f0.getGuid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 事件时间滚动窗口, 窗口长度10s   10 - (10 + 10) % 10
                // 设置自定义的trigger
                .trigger(MyEventTimeTrigger.create())
                // 设置自定义的evictor, 它会在窗口触发计算前, 对窗口中的e0x标记事件进行移除
                .evictor(MyTimeEvictor.of(Time.seconds(10)))
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

    }
}
/*
1,e01,10000,p01,10  [10,20)
1,e02,11000,p02,20
1,e02,12000,p03,40
1,e0x,13000,p03,40 ==> 这里会触发一次窗口
1,e04,16000,p05,50
1,e03,20000,p02,10  [20,30)  ==> 时间到达也会触发一次窗口
 */

class MyEventTimeTrigger extends Trigger<Tuple2<EventBean2, Integer>, TimeWindow> {

    private MyEventTimeTrigger() {}

    /*
    来一条数据时, 需要检查watermark是否已经越过窗口结束点需要触发
     */
    @Override
    public TriggerResult onElement(
            Tuple2<EventBean2, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        //如果窗口结束点 <= 当前watermark
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            //注册定时器, 定时器的触发时间为: 窗口的结束点时间
            ctx.registerEventTimeTimer(window.maxTimestamp());
            //todo 来一个e0x,触发计算
            if("e0x".equals(element.f0.getEventId())){
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }
    }

    /*
    当事件时间定时器的触发时间（窗口的结束点时间）到达了, 检查是否满足触发条件
    下面的方法, 是定时器在调用
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }
}


class MyTimeEvictor<W extends Window> implements Evictor<Object, W> {

    private final long windowSize;
    private final boolean doEvictAfter;

    public MyTimeEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyTimeEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    /**
     * 窗口触发前调用
     */
    @Override
    public void evictBefore(
            Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /*
      窗口触发后调用
     */
    @Override
    public void evictAfter(
            Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }
    /*
    元素移除的核心逻辑
     */
    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (!hasTimestamp(elements)) {
            return;
        }

        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            Tuple2<EventBean2,Integer> tuple = (Tuple2<EventBean2, Integer>) record.getValue();

            if (record.getTimestamp() <= evictCutoff || "e0x".equals(tuple.f0.getEventId())) {
                iterator.remove();
            }
        }
    }

    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
        Iterator<TimestampedValue<Object>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /*
    用于计算移除的时间截止点
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }

    @Override
    public String toString() {
        return "TimeEvictor(" + windowSize + ")";
    }

    @VisibleForTesting
    public long getWindowSize() {
        return windowSize;
    }


    public static <W extends Window> MyTimeEvictor<W> of(Time windowSize) {
        return new MyTimeEvictor<>(windowSize.toMilliseconds());
    }


    public static <W extends Window> MyTimeEvictor<W> of(Time windowSize, boolean doEvictAfter) {
        return new MyTimeEvictor<>(windowSize.toMilliseconds(), doEvictAfter);
    }
}
