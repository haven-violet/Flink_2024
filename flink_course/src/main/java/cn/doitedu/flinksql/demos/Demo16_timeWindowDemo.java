package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo16_timeWindowDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //bidtime | price | item | supplier_id |
        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9999);
        SingleOutputStreamOperator<Bid> s2 = s1.map(s -> {
            String[] split = s.split(",");
            return new Bid(split[0], Double.parseDouble(split[1]), split[2], split[3]);
        });

        // 将流转换成表
        tenv.createTemporaryView("t_bid", s2, Schema.newBuilder()
                        .column("bidtime", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .column("supplier_id", DataTypes.STRING())
                        .columnByExpression("rt", $("bidtime").toTimestamp())
                        .watermark("rt", "rt - interval '1' second")
                        .build());

        // 查询
        /*
        2020-04-15 08:05:00.000,4.4,milk,001
        2020-04-15 08:06:00.000,4.4,milk,001
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+
| op |                        bidtime |                          price |                           item |                    supplier_id |                      rt |                      wm |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+
| +I |        2020-04-15 08:05:00.000 |                            4.4 |                           milk |                            001 | 2020-04-15 08:05:00.000 |                  (NULL) |
| +I |        2020-04-15 08:06:00.000 |                            4.4 |                           milk |                            001 | 2020-04-15 08:06:00.000 | 2020-04-15 08:04:59.000 |

         */
        //tenv.executeSql(" select bidtime, price, item, supplier_id, rt, current_watermark(rt) as wm from t_bid ").print();

        // 每分钟, 计算最近5分钟的交易总额
        /*
        select
            window_start,
            window_end,
            sum(price) as price_amt
        from table(
            hop(table t_bid, descriptor(rt), interval '1' minutes, interval '5' minutes)
        )
        group by window_start, window_end
         */

        /*
        2020-04-15 08:05:00.000,4.4,milk,001
        2020-04-15 08:06:00.000,4.4,milk,001
        2020-04-15 08:10:00.000,4.4,milk,001
         */
        /*
+----+-------------------------+-------------------------+--------------------------------+
| op |            window_start |              window_end |                      price_amt |
+----+-------------------------+-------------------------+--------------------------------+
| +I | 2020-04-15 08:01:00.000 | 2020-04-15 08:06:00.000 |                            4.4 |
| +I | 2020-04-15 08:02:00.000 | 2020-04-15 08:07:00.000 |                            8.8 |
| +I | 2020-04-15 08:03:00.000 | 2020-04-15 08:08:00.000 |                            8.8 |
| +I | 2020-04-15 08:04:00.000 | 2020-04-15 08:09:00.000 |                            8.8 |
         */
        tenv.executeSql("        select \n" +
                "            window_start, \n" +
                "            window_end, \n" +
                "            sum(price) as price_amt \n" +
                "        from table(\n" +
                "            hop(table t_bid, descriptor(rt), interval '1' minutes, interval '5' minutes)\n" +
                "        )\n" +
                "        group by window_start, window_end")
                //.print()
        ;

        //每2分钟, 计算最近2分钟的交易总额
        /*
        select
            window_start,
            window_end,
            sum(price) as price_amt
        from table (
            tumble(table t_bid, descriptor(rt), interval '2' minutes)
        )
        group by window_start, window_end
        2020-04-15 08:05:00.000,4.4,milk,001
        2020-04-15 08:05:50.000,4.4,milk,001
        2020-04-15 08:06:00.000,4.4,milk,001
        2020-04-15 08:07:00.000,4.4,milk,001
        2020-04-15 08:08:00.000,4.4,milk,001
        2020-04-15 08:08:01.000,4.4,milk,001
+----+-------------------------+-------------------------+--------------------------------+
| op |            window_start |              window_end |                      price_amt |
+----+-------------------------+-------------------------+--------------------------------+
| +I | 2020-04-15 08:04:00.000 | 2020-04-15 08:06:00.000 |                            8.8 |
| +I | 2020-04-15 08:06:00.000 | 2020-04-15 08:08:00.000 |                            8.8 |
         */
        tenv.executeSql("        select\n" +
                "            window_start,\n" +
                "            window_end,\n" +
                "            sum(price) as price_amt\n" +
                "        from table (\n" +
                "            tumble(table t_bid, descriptor(rt), interval '2' minutes)\n" +
                "        )\n" +
                "        group by window_start, window_end")
                //.print()
        ;


        // 每2分钟, 计算今天以来的总成交额
        /*
        select
            window_start,
            window_end,
            sum(price) as price_amt
        from table (
            cumulate(table t_bid, descriptor(rt), interval '2' minutes, interval '24' hour)
        )
        group by window_start, window_end

2020-04-15 08:05:00.000,4.4,milk,001
2020-04-15 08:05:50.000,4.4,milk,001
2020-04-15 08:06:00.000,4.4,milk,001
2020-04-15 08:07:00.000,4.4,milk,001
2020-04-15 08:08:00.000,4.4,milk,001
2020-04-15 08:08:01.000,4.4,milk,001
+----+-------------------------+-------------------------+--------------------------------+
| op |            window_start |              window_end |                      price_amt |
+----+-------------------------+-------------------------+--------------------------------+
| +I | 2020-04-15 00:00:00.000 | 2020-04-15 08:06:00.000 |                            8.8 |
| +I | 2020-04-15 00:00:00.000 | 2020-04-15 08:08:00.000 |                           17.6 |
         */
        tenv.executeSql("   select\n" +
                "            window_start,\n" +
                "            window_end,\n" +
                "            sum(price) as price_amt \n" +
                "        from table (\n" +
                "            cumulate(table t_bid, descriptor(rt), interval '2' minutes, interval '24' hour)\n" +
                "        )\n" +
                "        group by window_start, window_end")
                //.print()
        ;


        //每10分钟计算一次, 最近10分钟内交易总额最大的前3个供应商及其交易单数
        /*
        select
            window_start,
            window_end,
            supplier_id,
            price_amt,
            exchange_cnt,
            rn
        from (
            select
                window_start,
                window_end,
                supplier_id,
                price_amt,
                exchange_cnt,
                row_number() over(partition by window_start, window_end order by price_amt desc) rn
            from (
                select
                    window_start,
                    window_end,
                    supplier_id,
                    sum(price) as price_amt,
                    count(1) as exchange_cnt
                from table (
                    tumble(table t_bid, descriptor(rt), interval '10' minutes, interval '10' minutes)
                )
                group by
                    window_start,
                    window_end,
                    supplier_id
            ) a
        ) b where rn <= 3

2020-04-15 08:05:00.000,4.4,milk,001
2020-04-15 08:05:00.000,5.4,milk,002
2020-04-15 08:05:00.000,6.4,milk,003
2020-04-15 08:05:00.000,7.4,milk,004
2020-04-15 08:05:50.000,8.4,milk,001

2020-04-15 08:15:50.000,8.4,milk,001



         */
        tenv.executeSql("        select\n" +
                "            window_start,\n" +
                "            window_end,\n" +
                "            supplier_id,\n" +
                "            price_amt,\n" +
                "            exchange_cnt,\n" +
                "            rn\n" +
                "        from (\n" +
                "            select\n" +
                "                window_start,\n" +
                "                window_end,\n" +
                "                supplier_id,\n" +
                "                price_amt,\n" +
                "                exchange_cnt,\n" +
                "                row_number() over(partition by window_start, window_end order by price_amt desc) rn\n" +
                "            from (\n" +
                "                select\n" +
                "                    window_start,\n" +
                "                    window_end,\n" +
                "                    supplier_id,\n" +
                "                    sum(price) as price_amt,\n" +
                "                    count(1) as exchange_cnt\n" +
                "                from table (\n" +
                "                    tumble(table t_bid, descriptor(rt), interval '10' minutes)\n" +
                "                )\n" +
                "                group by\n" +
                "                    window_start,\n" +
                "                    window_end,\n" +
                "                    supplier_id\n" +
                "            ) a\n" +
                "        ) b where rn <= 3")
                .print();


    }



    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bid {
        private String bidtime;
        private Double price;
        private String item;
        private String supplier_id;
    }
}
