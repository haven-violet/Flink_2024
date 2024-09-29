package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _24_state_keyedState_demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //开启状态数据的checkpoint机制(快照的周期, 快照的模式)
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        //开启快照后,就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/checkpoint/");
        //开启task级别故障自动failover
        //默认是，不会自动failover； 一个task故障了，整个job就失败了
        //使用的重启策略是: 固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));


        //需要使用map达到一个效果:
        //就是每来一条数据,都要拼接此前达到过的所有字符串输出
        /**
         * TODO
         *  算子状态一般是用在source算子上，它是每个subTask并行度保存一份状态
         *      数据重分配的时候会重新分配, 需要实现 CheckpointFunction
         *  键控状态是每个key都有自己的一份状态，和并行度没有关系
         *      数据重分配的时候，只是key % 并行度;这种区别
         *      需要实现RichFunction的函数
         */
        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 9999);
        source
//              .keyBy(s -> "0")
              .keyBy(s -> s)
              .map(new RichMapFunction<String, String>() {
                  ListState<String> listState;
                  @Override
                  public void open(Configuration parameters) throws Exception {
                      //创建一个listState状态管理器
                      listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("strings", String.class));
                  }

                  @Override
                  public String map(String value) throws Exception {
                      listState.add(value);
                      StringBuilder sb = new StringBuilder();
                      for (String s : listState.get()) {
                          sb.append(s);
                      }
                      return sb.toString();
                  }
              }).setParallelism(2).print().setParallelism(2);


/*
2> a
1> b
1> c
1> e
1> f
1> ff
1> ee
2> aa
2> d
2> dd
 */
        env.execute();
    }
}
