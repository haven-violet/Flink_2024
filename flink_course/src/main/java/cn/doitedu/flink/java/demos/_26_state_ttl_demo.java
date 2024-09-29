package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _26_state_ttl_demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        /**
         * 设置要使用的状态后端  内存+磁盘 对象形式
         */
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend); // 使用HashMapStateBackend 作为状态后端

        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        //env.setStateBackend(embeddedRocksDBStateBackend); // 设置 EmbeddedRocksDBStateBackend 作为状态后端

        // 开启状态数据的checkpoint机制 （快照的周期，快照的模式）
        // interval 多久生成barrier插入source数据中;
        // exactly_once 就是恰好一次语义，这里是多流的checkpoint barrier到达时机不一致，是应该阻塞先到达的呢？
        // 这里是先阻塞先到达的数据，放入缓存里面，等到慢的另外一条流的barrier到达后，一起做快照持久化
        // 一般都是推荐使用 Exactly_once
        // 另外一个at_least_once是，先到的先checkpoint, 并且把后面到的数据也存储下来，任务失败重启一并恢复
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 开启快照后, 就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/checkpoint/");
        // 开启 task级别故障自动 failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 9999);

        // 需要使用map算子来达到一个效果
        // 每来一条数据(字符串)，输出 该字符串拼接此前到达过的所有字符串
        source
            .keyBy(s -> "0")
            .map(new RichMapFunction<String, String>() {
                ListState<String> listState;
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);

                    //获取一个单值结构的状态存储器, 并设置TTL参数
                    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(1000))//配置数据的存活时长为1s
                            .setTtl(Time.milliseconds(4000)) // 配置数据的存活时长为4s
//                            .updateTtlOnReadAndWrite() // 读、写，都导致该条数据的ttl计时重置
//                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 设置ttl计时重置的策略
//                            .updateTtlOnCreateAndWrite() //当插入和更新的时候, 导致该条数据的ttl计时重置
                            //不允许返回已经过期但是尚未被清理的数据
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            //允许返回已经过期但是尚未被清理的数据
                           // .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                            //ttl计时的时间语义: 设置为处理时间
                            .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                            .useProcessingTime() // ttl计时的时间语义: 设置为处理时间
                            //下面3种过期数据检查清除策略, 不是覆盖的关系, 而是添加关系
                            // 增量清理(每当一条状态数据被访问,则会检查这条状态数据的ttl是否超时, 是就删除)
                            .cleanupIncrementally(10, false)
//                            // 全量快照清理策略(在checkpoint的时候, 保存到快照文件中的只包含未过期的状态数据, 但是它并不会清理算子本地的状态数据)
//                            .cleanupFullSnapshot()
//                            // 在rocksdb的compact机制中添加过期数据过滤器, 以在compact过程中清理掉过期状态数据
//                            .cleanupInRocksdbCompactFilter(1000)
//                            //
//                            .disableCleanupInBackground()
                            .build();

                    ListStateDescriptor<String> listStateDescriptor1 = new ListStateDescriptor<>("strings", String.class);
                    listStateDescriptor1.enableTimeToLive(ttlConfig);
                    listState = getRuntimeContext().getListState(listStateDescriptor1);

                    ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("vstate", String.class);
                    valueStateDescriptor.enableTimeToLive(ttlConfig); // 开启TTL管理
                    // 本状态管理器就会执行ttl管理
                    // ValueState<String> valueState = getRuntimeContext().getState(valueStateDescriptor);
                    ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("lst", String.class);
                    listStateDescriptor.enableTimeToLive(ttlConfig);
                    ListState<String> listState1 = getRuntimeContext().getListState(listStateDescriptor);
                    //获取一个map结构的状态存储器
                    MapState<String, String> mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("xx", String.class, String.class));
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
            }).setParallelism(1)
            .print().setParallelism(1);



        env.execute();
    }
}
