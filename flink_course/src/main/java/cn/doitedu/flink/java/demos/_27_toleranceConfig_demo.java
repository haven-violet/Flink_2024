package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


public class _27_toleranceConfig_demo {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        /**
         * checkpoint相关配置
         */
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE); //传入两个最基本ck参数: 间隔时长, ck模式
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://doit01:8020/ckpt");
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(10000));// 设置ck对齐的超时时长
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置ck算法模式
        checkpointConfig.setCheckpointInterval(2000); // 设置ck的间隔时长
        //用于非对齐算法模式下，在job恢复时让各个算子自动抛弃掉ck-5中飞行数据
        checkpointConfig.setCheckpointIdOfIgnoredInFlightData(5); // 用于非对齐算法模式下, 在job恢复时让各个算子自动抛弃掉ck-5中飞行数据
        //job cancel掉时, 保留最后一次ck数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //是否强制使用 非对齐的checkpoint模式
        checkpointConfig.setForceUnalignedCheckpoints(false);
        //允许在系统中同时存在的飞行中(未完成的)ck数
        checkpointConfig.setMaxConcurrentCheckpoints(5);
        //设置两次ck之间的最小时间间隔,用于防止checkpoint过多地占用算子的处理时间
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        //一个算子在一次checkpoint执行过程中的总耗费时长上限
        checkpointConfig.setCheckpointTimeout(3000);
        //允许的checkpoint失败最大次数
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        /**
         * task失败自动重启策略配置
         */
        RestartStrategies.RestartStrategyConfiguration restartStrategy = null;
        //固定、延迟重启(参数1: 故障重启最大次数; 参数2: 两次重启之间的延迟间隔)
        restartStrategy = RestartStrategies.fixedDelayRestart(5, 2000);

        //默认的故障重启策略: 不重启(只要有task失败，整个job就失败)
        restartStrategy = RestartStrategies.noRestart();


        env.execute();
    }
}
