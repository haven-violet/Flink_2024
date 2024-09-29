package cn.doitedu.flink.java.demos;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

public class _23_state_OperatorState_demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        /**
         * TODO 状态要想生效需要搭配着 checkpoint检查点，
         *      由checkpoint来将状态持久化起来，这样才能在下次启动的时候加载起来
         */
        //开启状态数据的checkpoint机制 (快照的周期，快照的模式)
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        //开启快照后,就需要指定快照数据的持久化存储位置
        //env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://doit01:8020/checkpoint/"));
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/checkpoint/");

        //开启 task级别故障自动failover
        //默认是, 不会自动failover; 一个task故障了, 整个job就失败了
        //env.setRestartStrategy(RestartStrategies.noRestart());
        //使用的重启策略是: 固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));


        /**
         * TODO 算子状态 OperatorState, 不需要keyBy就能使用
         */
        DataStreamSource<String> source = env.socketTextStream("192.168.157.102", 8888);
        source.map(new StateMapFunction()).print();


        env.execute();
    }
}

/*
要使用operator state, 需要让用户自己的Function类去实现 CheckpointedFunction
然后在其中的方法 initializeState中,去拿到operator state存储器
*/
class StateMapFunction implements MapFunction<String, String>, CheckpointedFunction{
    ListState<String> listState;
    @Override
    public String map(String value) throws Exception {
        //将本条数据放入ListState状态存储器中管理
        listState.add(value);

        /**
         * 故意埋一个异常，来测试task级别自动容错效果
         */
        if("x".equals(value) && RandomUtils.nextInt(1, 15) % 4 == 0) {
            throw new Exception("哈哈哈, 出错了");
        }


        Iterable<String> strings = listState.get();
        StringBuilder sb = new StringBuilder();
        for (String str : strings) {
            sb.append(str);
        }
        return sb.toString();
    }

    /**
     * 系统对状态数据做快照(持久化)时会调用的方法, 用户利用这个方法, 在持久化前, 对状态数据做一些操控
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //System.out.println("checkpoint 触发了, checkpointId : " + context.getCheckpointId());
    }

    /**
     * 算子任务在启动之初, 会调用下面的方法, 来为用户进行状态数据初始化
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 从方法提供的context中拿到一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        //算子状态存储器, 只提供List数据结构来为用户存储数据
        ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("strings", String.class);
        //getListState方法，在task失败后, task自动重启时, 会帮用户自动加载最近一次的快照状态数据
        //如果是job重启, 则不会自动加载此前的快照状态数据
        listState = operatorStateStore.getListState(stateDescriptor); // 在状态管理器上调用get方法, 得到具体结构的状态管理器


        /**
         * unionListState 和 普通 ListState的区别:
         * unionListState的快照存储数据,在系统重启后, list数据的重分配模式为: 广播模式: 在每个subtask上都拥有一份完整的数据
         * ListState的快照存储数据, 在系统重启后, list数据的重分配模式为: round-robin: 轮询平均分配
         */
        //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);
    }
}