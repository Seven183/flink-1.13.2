package task.join;

import domain.FactOrderItem;
import domain.Goods;
import domain.OrderItem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import source.GoodsSource;
import source.OrderItemSource;
import utils.EnvConfig;
import utils.PropertyUtils;
import watermark.GoodsWatermark;
import watermark.OrderItemWatermark;

import java.math.BigDecimal;

public class FlinkIntervalJoin {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.getEnvConfig(env);
        PropertyUtils.init(args[0]);

        //TODO 1.source
        //商品数据流
        DataStreamSource<Goods> goodsDS = env.addSource(new GoodsSource());
        //订单数据流
        DataStreamSource<OrderItem> OrderItemDS = env.addSource(new OrderItemSource());
        //给数据添加水印(这里简单一点直接使用系统时间作为事件时间)
        /*
         SingleOutputStreamOperator<Order> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))//指定maxOutOfOrderness最大无序度/最大允许的延迟时间/乱序时间
                        .withTimestampAssigner((order, timestamp) -> order.getEventTime())//指定事件时间列
        );
         */
        SingleOutputStreamOperator<Goods> goodsDSWithWatermark = goodsDS.assignTimestampsAndWatermarks(new GoodsWatermark());
        SingleOutputStreamOperator<OrderItem> OrderItemDSWithWatermark = OrderItemDS.assignTimestampsAndWatermarks(new OrderItemWatermark());


        //TODO 2.transformation---这里是重点
        //商品类(商品id,商品名称,商品价格)
        //订单明细类(订单id,商品id,商品数量)
        //关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
        SingleOutputStreamOperator<FactOrderItem> resultDS = goodsDSWithWatermark.keyBy(Goods::getGoodsId)
                .intervalJoin(OrderItemDSWithWatermark.keyBy(OrderItem::getGoodsId))
                //join的条件:
                // 条件1.id要相等
                // 条件2. OrderItem的时间戳 - 2 <=Goods的时间戳 <= OrderItem的时间戳 + 1
                .between(Time.seconds(-2), Time.seconds(1))
                //ProcessJoinFunction<IN1, IN2, OUT>
                .process(new ProcessJoinFunction<Goods, OrderItem, FactOrderItem>() {
                    @Override
                    public void processElement(Goods left, OrderItem right, Context ctx, Collector<FactOrderItem> out) throws Exception {
                        FactOrderItem result = new FactOrderItem();
                        result.setGoodsId(left.getGoodsId());
                        result.setGoodsName(left.getGoodsName());
                        result.setCount(new BigDecimal(right.getCount()));
                        result.setTotalMoney(new BigDecimal(right.getCount()).multiply(left.getGoodsPrice()));
                        out.collect(result);
                    }
                });

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();
    }
}
