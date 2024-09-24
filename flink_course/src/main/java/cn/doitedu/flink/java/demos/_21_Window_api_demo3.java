package cn.doitedu.flink.java.demos;

import cn.doitedu.flink.java.bean.TaoBaoBean;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class _21_Window_api_demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        SingleOutputStreamOperator<TaoBaoBean> taobaoStream = env.socketTextStream("192.168.157.102", 8888).map(s -> {
            String[] arr = s.split(",");
            return new TaoBaoBean(arr[0], Integer.parseInt(arr[1]), Integer.parseInt(arr[2]));
        })
        .returns(TaoBaoBean.class)
        .assignTimestampsAndWatermarks(WatermarkStrategy
                .<TaoBaoBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<TaoBaoBean>() {
                    @Override
                    public long extractTimestamp(TaoBaoBean element, long recordTimestamp) {
                        return element.getTimstamp() * 1000l;
                    }
                })
        );

        /*
        TODO 定时器只有在 process能够调用
             keyBy后, 每个key 都会有自己的定时器
         */
        taobaoStream
            .keyBy(TaoBaoBean::getName)
            .process(new KeyedProcessFunction<String, TaoBaoBean, String>() {
                @Override
                public void processElement(TaoBaoBean value, KeyedProcessFunction<String, TaoBaoBean, String>.Context ctx, Collector<String> out) throws Exception {
                    //数据中提取出来的事件时间
                    Long currentEventTime = ctx.timestamp();
                    //定时器
                    TimerService timerService = ctx.timerService();
                    //注册定时器: 事件时间,  这里意思是来了一条数据后, 当watermark推进到了>=5s的时候会触发定时操作
                    timerService.registerEventTimeTimer(5000l);
                    System.out.println("当前key是 "+ ctx.getCurrentKey() +"当前时间是" + currentEventTime + ", 注册了一个5s定时器");
                }

                /**
                 * 时间进展到定时器注册的时间, 调用该方法
                 * @param timestamp 当前时间进展
                 * @param ctx 上下文
                 * @param out 采集器
                 * @throws Exception
                 */
                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<String, TaoBaoBean, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                    super.onTimer(timestamp, ctx, out);
                    System.out.println("现在key是 "+ ctx.getCurrentKey() +" 现在时间是" + timestamp + " 定时器触发" );
                }
            }).print();


        env.execute();
    }
}
