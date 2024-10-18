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

public class Demo20_temporal_join {
    public static void main(String[] args) throws Exception {
        // TODO  temporary join a表数据 -> b表数据最新时间版本的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        /**
         * 订单id, 币种, 金额, 订单时间
         * 1,a,100,167438436400
         */
        DataStreamSource<String> s1 = env.socketTextStream("192.168.157.102", 9998);
        SingleOutputStreamOperator<Order> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
        });

        /**
         * 创建主表(需要声明处理时间属性字段)
         */
        tenv.createTemporaryView("orders", ss1, Schema.newBuilder()
                .column("orderId", DataTypes.INT())
                .column("currency", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("orderTime", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(orderTime,3)") // 定义处理时间属性字段
                .watermark("rt", "rt")
                .build());

        //tenv.executeSql("select orderId, currency, price, orderTime, rt from orders").print();
        //创建 temporal表
        /*
        create table currency_rate (
            currency string,
            rate double,
            update_time bigint,
            rt as to_timestamp_ltz(update_time, 3),
            watermark for rt as rt - interval '0' second,
            primary key(currency) not enforced
        ) with (
            'connector' = 'mysql-cdc',
            'hostname' = '192.168.157.102',
            'port' = '3306',
            'username' = 'root',
            'password' = 'root',
            'database-name' = 'flinktest',
            'table-name' = 'currency_rate'
        )
         */
        tenv.executeSql(
      "        create table currency_rate (\n" +
                "            currency string,\n" +
                "            rate double,\n" +
                "            update_time bigint,\n" +
                "            rt as to_timestamp_ltz(update_time, 3),\n" +
                "            watermark for rt as rt - interval '0' second,\n" +
                "            primary key(currency) not enforced\n" +
                "        ) with (\n" +
                "            'connector' = 'mysql-cdc',\n" +
                "            'hostname' = '192.168.157.1',\n" +
                "            'port' = '3306',\n" +
                "            'username' = 'root',\n" +
                "            'password' = 'root',\n" +
                "            'database-name' = 'life_db',\n" +
                "            'table-name' = 'currency_rate'\n" +
                "        )");

        // temporary 关联查询
        /*
        select
            orders.orderId,
            orders.currency,
            order.price,
            order.orderTime,
            rate
        from orders left join currency_rate for system_time as of orders.rt
            on orders.currency = currency_rate.currency
         */
        tenv.executeSql(
      "        select \n" +
                "            orders.orderId,\n" +
                "            orders.currency, \n" +
                "            orders.price,\n" +
                "            orders.orderTime, \n" +
                "            rate\n" +
                "        from orders " +
                "        left join currency_rate for system_time as of orders.rt \n" +
                "            on orders.currency = currency_rate.currency").print();
/*

TODO  temporary join a表一条数据, 它会去读取 b表对应时间的最新版本数据, 如果b表没有a表对应时间的数据, 那么 a表数据先等待着, 等到b表推进到了该时间才会出结果
1,dollor,1,2300
1,dollor,1,2333
1,dollor,1,2400
1,dollor,2,2500
1,dollor,3,2600
1,dollor,4,3000
1,dollor,5,3300


dollor  7.7 2400
dollor	7.9	3000
dollor	9.8	3900

+----+-------------+--------------------------------+--------------------------------+----------------------+--------------------------------+
| op |     orderId |                       currency |                          price |            orderTime |                           rate |
+----+-------------+--------------------------------+--------------------------------+----------------------+--------------------------------+
| +I |           1 |                         dollor |                            1.0 |                 2300 |                         (NULL) |b表时间未到
| +I |           1 |                         dollor |                            1.0 |                 2333 |                         (NULL) |b表时间未到
| +I |           1 |                         dollor |                            1.0 |                 2400 |                            7.7 |b表最新2400->7.7
| +I |           1 |                         dollor |                            2.0 |                 2500 |                            7.7 |b表[2400,3000)->7.7
| +I |           1 |                         dollor |                            3.0 |                 2600 |                            7.7 |b表[2400,3000)->7.7
| +I |           1 |                         dollor |                            4.0 |                 3000 |                            7.9 |b表最新3000->7.9
| +I |           1 |                         dollor |                            5.0 |                 3300 |                            7.9 |b表[3000,3900)->7.9

 */


        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        //订单id,币种,金额,订单时间
        public int orderId;
        public String currency;
        public double price;
        public long orderTime;
    }
}
