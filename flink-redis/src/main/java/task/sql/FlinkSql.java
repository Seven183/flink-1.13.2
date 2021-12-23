package task.sql;

import domain.Goods;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSql {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStream<Goods> orderA = env.fromCollection(Arrays.asList(
                new Goods("1", "beer", new BigDecimal(30)),
                new Goods("2", "diaper", new BigDecimal(40)),
                new Goods("3", "rubber", new BigDecimal(50))));

        DataStream<Goods> orderB = env.fromCollection(Arrays.asList(
                new Goods("4", "beer", new BigDecimal(60)),
                new Goods("5", "diaper", new BigDecimal(70)),
                new Goods("6", "rubber", new BigDecimal(80))));

        //TODO 2.transformation
        // 将DataStream数据转Table和View,然后查询
        Table tableA = tenv.fromDataStream(orderA, $("goodsId"), $("goodsName"), $("goodsPrice"));
        tenv.createTemporaryView("tableB", orderB, $("goodsId"), $("goodsName"), $("goodsPrice"));

        String sql = "select * from "+tableA+" where goodsPrice > 40 \n" +
                "union \n" +
                " select * from tableB where goodsPrice > 70";

        String sql2 = "select goodsId,goodsName,sum(goodsPrice) as goodsPrice\n " +
                "from tableB\n " +
                "group by goodsId,goodsName";

        Table resultTable = tenv.sqlQuery(sql2);

        //将Table转为DataStream
        DataStream<Tuple2<Boolean, Goods>> resultDS = tenv.toRetractStream(resultTable, Goods.class);//union使用toRetractStream

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();
    }
}
