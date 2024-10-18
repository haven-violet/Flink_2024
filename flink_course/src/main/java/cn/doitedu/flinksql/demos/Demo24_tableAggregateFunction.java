package cn.doitedu.flinksql.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * 自定义表聚合函数示例
 * 什么叫做表聚合函数
 * 1,male,zs,88
 * 2,male,bb,99
 * 3,male,cc,76
 * 4,female,dd,78
 * 5,female,ee,92
 * 6,female,ff,86
 * -- 求每种性别中,分数最高的两个成绩
 * -- 常规写法
 * select
 *  *
 * from (
 *      select
 *       gender,
 *       score,
 *       row_number() over(partition by gender order by score desc) as rn
 *      from  t
 * ) where rn <= 2
 * -- 如果有一种聚合函数, 能在分组聚合的模式中, 对每组数据输出多行多列聚合结果
 * select
 *  gender,
 *  top2(score)
 * from t
 * group by gender
 *
 * male,88
 * male,99
 * female,92
 * female,86
 */
public class Demo24_tableAggregateFunction {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = tenv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1, "male", 67),
                Row.of(2, "male", 88),
                Row.of(3, "male", 98),
                Row.of(4, "female", 99),
                Row.of(5, "female", 84),
                Row.of(6, "female", 89)
        );
        tenv.createTemporaryView("t", table);

        //用一个聚合函数直接求出每种性别中最高的两个成绩
        table
                .groupBy($("gender"))
                .flatAggregate(call(MyTop2.class, $("score")))
                .select($("gender"), $("score_top"), $("rank_no"))
                .execute().print();


    }

    public static class MyAccumulator {
        public double first;
        public double second;
    }

    @FunctionHint(output = @DataTypeHint("ROW<score_top DOUBLE, rank_no INT>"))
    public static class MyTop2 extends TableAggregateFunction<Row, MyAccumulator> {

        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator acc = new MyAccumulator();
            acc.first = Double.MIN_VALUE;
            acc.second = Double.MIN_VALUE;
            return acc;
        }

        /**
         * 累加更新逻辑
         */
        public void accumulate(MyAccumulator acc, Double score) {
            if(score > acc.first) {
                acc.second = acc.first;
                acc.first = score;
            } else if (score > acc.second){
                acc.second = score;
            }
        }

        public void merge(MyAccumulator acc, Iterable<MyAccumulator> it) {
            for (MyAccumulator otherAcc : it) {

            }
        }

        /**
         * 输出结果: 可以输出多行、多列
         * @param acc
         * @param out
         */
        public void emitValue(MyAccumulator acc, Collector<Row> out){
            if(acc.first != Double.MIN_VALUE) {
                out.collect(Row.of(acc.first, 1));
            }
            if(acc.second != Double.MIN_VALUE){
                out.collect(Row.of(acc.second, 2));
            }
        }
    }

}
