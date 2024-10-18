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
 * 各种窗口代码join
 */
public class Demo17_timeWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, envSetting);
        env.setParallelism(1);
        //TODO 并行度会影响watermark的推进, 进而影响窗口window触发, 所以这里需要设置为 1个并行度

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
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });

        //创建两个表
        tenv.createTemporaryView("t_left", ss1,  Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build()
        );

        tenv.createTemporaryView("t_right", ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        //INNER
        /*
        select
            a.f0, a.f1, a.f2, b.f0, b.f1
        from (
            select * from table( tumble(table t_left, descriptor(rt), interval '10' second) )
        ) a join (
            select * from table( tumble(table t_right, descriptor(rt), interval '10' second) )
        ) b on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0
         */
        tenv.executeSql(
      "        select \n" +
                "            a.f0, a.f1, a.f2, b.f0, b.f1 \n" +
                "        from (\n" +
                "            select * from table( tumble(table t_left, descriptor(rt), interval '10' second) )\n" +
                "        ) a join (\n" +
                "            select * from table( tumble(table t_right, descriptor(rt), interval '10' second) )\n" +
                "        ) b on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0 ")
        //        .print()
        ;

        //left / right / full join
        /*
        select a.f0, a.f1, a.f2, b.f0, b.f1
        from (
            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))
        ) a full join (
            select * from table(tumble(table t_right, descriptor(rt), interval '10' second))
        ) b on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0
         */
        tenv.executeSql(
      "        select a.f0, a.f1, a.f2, b.f0, b.f1  \n" +
                "        from (\n" +
                "            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
                "        ) a full join (\n" +
                "            select * from table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
                "        ) b on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0 ")
        //        .print()
        ;


        // semi join => where .. in ..
        /*
        select a.*
        from (
            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))
        ) a where a.f0 in (
            select f0 from (
                select * from table(tumble(table t_right, descriptor(rt), interval '10' second))
            ) b where a.window_start = b.window_start and a.window_end = b.window_end
        )
         */
        tenv.executeSql("        select a.f0, a.f1, a.f2\n" +
                "        from (\n" +
                "            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
                "        ) a where a.f0 in (\n" +
                "            select f0 from (\n" +
                "                select * from table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
                "            ) b where a.window_start = b.window_start and a.window_end = b.window_end \n" +
                "        )")
        //        .print()
        ;

        // semi join => where .. not in ..
        /*
        select
            a.f0, a.f1, a.f2, a.rt
        from (
            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))
        ) a where a.f0 not in (
            select f0 from (
                select * from table(tumble(table t_right, descriptor(rt), interval '10' second))
            ) b
        )
         */
        tenv.executeSql("        select\n" +
                "            a.f0, a.f1, a.f2, a.rt\n" +
                "        from (\n" +
                "            select * from table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
                "        ) a where a.f0 not in (\n" +
                "            select f0 from (\n" +
                "                select * from table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
                "            ) b  where a.window_start = b.window_start and a.window_end = b.window_end \n" +
                "        )").print();



        env.execute();
    }
}
