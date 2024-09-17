package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _18_ChannelSelector_Partitioner_Demo {
    /**
     * TODO  Flink
     *      task  类  代码没有运行起来时候
     *      subtask task运行起来的运行实例,会占用线程资源
     *      并行度   =>  subtask 指定需要多少个并行实例来 分布式运行 task代码   100条数据，5个subtask, 5个并行度, 每个并行度分配20条数据，同时运行这样节省了时间
     *      TaskManager 进程  很多个slot可以运行的槽位, 每个slot槽位只能运行一个task中的一个subtask
     *          同task下中subtask不能占用同一个slot槽位,因为是并行实例需要同时运行
     *          但是可以不同task的一个subtask可以共享slot槽位
     *          因为执行有先后顺序之分,所以可以共享槽位
     *      算子链  能够减少线程个数，并且还减少了网络传输
     *          1. 能够上下游task并行实例能够 one to one
     *          2. 上下游算子并行度一样
     *          3. 是否设置不同槽位共享组, 默认大家都是相同槽位共享组，如果设置了的话,那么map{2} g1  flatmap{2} g2
     *              虽然map和flatMap能够一对一并且并行度都是2,但是设置不同槽位共享组，那么他们是不能绑定在一块，会拆分成2个task
     *      operator chain
     *      .disableChain
     *      .startNewChanin
     *      .slotGroup
     *      上下游task并行实例数据传输关系
     *          a0, a1, a2
     *          b0, b2
     *      rebalance()默认轮循
     *      shuffle() 随机 Random().nextInt()
     *      keyBy()  Hash  key的hash
     *
     */
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9999);
// socket 1 -rebalance-> map 12/16 -rebalance-> flatMap 2 -rebalance-> map 4
//          -hash-> keyBy + filter 4 -rebalance-> print 2
        SingleOutputStreamOperator<String> s2 = s1.map(s -> s.toUpperCase())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> collector) throws Exception {
                        String[] arr = value.split(",");
                        for (int i = 0; i < arr.length; i++) {
                            collector.collect(arr[i]);
                        }
                    }
                }).setParallelism(2);

        SingleOutputStreamOperator<String> s3 = s2.map(s -> s.toLowerCase()).setParallelism(4);

        SingleOutputStreamOperator<String> s4 = s3.keyBy(s -> s.toLowerCase())
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(s + ">");
                    }
                }).setParallelism(4)
                .slotSharingGroup("g2")
                ;
                //.disableChaining();

        SingleOutputStreamOperator<String> s5 = s4.filter(s -> s.startsWith("b")).setParallelism(4).slotSharingGroup("g3");
                //.startNewChain();

        s5.print().setParallelism(2);

        env.execute();
    }
}
