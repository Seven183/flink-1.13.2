package task.join;

import domain.FactOrderItem;
import domain.Goods;
import domain.OrderItem;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import source.GoodsSource;
import source.OrderItemSource;
import utils.EnvConfig;
import utils.PropertyUtils;
import watermark.GoodsWatermark;
import watermark.OrderItemWatermark;

import java.math.BigDecimal;

public class FlinkWindowJoin {
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

        SingleOutputStreamOperator<Goods> goodsDSWithWatermark = goodsDS.assignTimestampsAndWatermarks(new GoodsWatermark());
        SingleOutputStreamOperator<OrderItem> OrderItemDSWithWatermark = OrderItemDS.assignTimestampsAndWatermarks(new OrderItemWatermark());

        //TODO 2.transformation---这里是重点
        //商品类(商品id,商品名称,商品价格)
        //订单明细类(订单id,商品id,商品数量)
        //关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
        DataStream<FactOrderItem> resultDS = goodsDSWithWatermark.join(OrderItemDSWithWatermark)
                .where(Goods::getGoodsId)
                .equalTo(OrderItem::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //<IN1, IN2, OUT>
                .apply(new JoinFunction<Goods, OrderItem, FactOrderItem>() {
                    @Override
                    public FactOrderItem join(Goods first, OrderItem second) throws Exception {
                        FactOrderItem result = new FactOrderItem();
                        result.setGoodsId(first.getGoodsId());
                        result.setGoodsName(first.getGoodsName());
                        result.setCount(new BigDecimal(second.getCount()));
                        result.setTotalMoney(new BigDecimal(second.getCount()).multiply(first.getGoodsPrice()));
                        return result;
                    }
                });


        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();
    }
}
