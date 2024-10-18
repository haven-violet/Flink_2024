package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 常规join示例
 *  常规join的底层实现, 是通过在用状态来缓存两表数据实现的
 *  所以, 状态体积可能持续膨胀, 为了安全起见, 可以设置状态的TTL时长, 来控制状态的体积上限
 */
public class Demo18_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);
        env.setParallelism(1);
        // 设置table环境中的状态ttl时长
        tenv.getConfig().getConfiguration().setLong("table.exec.state.ttl", 60*60*1000L);

        /**
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,12000
         */
        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9998);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {});

        /**
         * 1,bj,1000
         * 2,sh,2000
         * 4,xa,2600
         * 5,yn,12000
         */
        DataStreamSource<String> s2 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss2 = s2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {});


        //创建2张表
        tenv.createTemporaryView("t_left", ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        tenv.createTemporaryView("t_right", ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        //interval join  a.dt  在 b表的时间范围内
        /**
         * select *
         * from t_left a
         * join t_right b
         *  on a.f0 = b.f0 and a.rt between b.rt - interval '2' second and b.rt
         *  a   b
         *  1   1
         *  2   2
         *  3   3
         *  4   4
         *  5   5                   进行到了5
         *  6   6                   3 - 5
         *  7   7                   4 - 6
         *  8   8                   5 - 7
         *  9   9
         */
        tenv.executeSql("select a.f0, a.f1, a.f2, b.f0, b.f1, b.f2\n" +
                " from t_left a\n" +
                " join t_right b\n" +
                " on a.f0 = b.f0 and a.rt between b.rt - interval '2' second and b.rt")
        //        .print()
        ;
        //TODO  interval join 就是普通regular join, 只是限定了 a.rt -> [b.rt - 2s, b.rt] 范围内 join
/*
t_left
    4,d,5000
t_right
    4,xa,2000
    4,xa,3000
    4,xa,4000
    4,xa,4900
    4,xa,4999
    4,xa,5000  开始 start
    4,xa,5100
    4,xa,6100
    4,xa,7000  结束 end
    4,xa,7001

+----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+----------------------+
| op |                             f0 |                             f1 |                   f2 |                            f00 |                            f10 |                  f20 |
+----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+----------------------+
| +I |                              4 |                              d |                 5000 |                              4 |                             xa |                 5000 |
| +I |                              4 |                              d |                 5000 |                              4 |                             xa |                 5100 |
| +I |                              4 |                              d |                 5000 |                              4 |                             xa |                 6100 |
| +I |                              4 |                              d |                 5000 |                              4 |                             xa |                 7000 |

 */
        // TODO regular join
        //    1. left join || join
        /*
        select a.f0, a.f1, a.f2, b.f0, b.f1
        from t_left a
        left join t_right b on a.f0 = b.f0

+----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
| op |                             f0 |                             f1 |                   f2 |                            f00 |                            f10 |
+----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
| +I |                              4 |                              d |                 5000 |                         (NULL) |                         (NULL) |
| -D |                              4 |                              d |                 5000 |                         (NULL) |                         (NULL) |
| +I |                              4 |                              d |                 5000 |                              4 |                             xa |
| +I |                              5 |                              e |                 6000 |                         (NULL) |                         (NULL) |

         */
        tenv.executeSql(
      "        select a.f0, a.f1, a.f2, b.f0, b.f1  \n" +
                "        from t_left a \n" +
                "        left join t_right b on a.f0 = b.f0").print();

        env.execute();
    }
}
