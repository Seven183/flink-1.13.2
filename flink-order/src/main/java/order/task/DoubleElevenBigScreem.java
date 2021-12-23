package order.task;

import lombok.extern.slf4j.Slf4j;
import order.domain.Order;
import order.domain.OrderProduct;
import order.join.OrderJoinOrderProduct;
import order.result.OrderBrandResult;
import order.result.OrderResult;
import order.sink.redisMapper.OrderBrandResultMapper;
import order.sink.redisMapper.OrderBrandSalesTop5Mapper;
import order.sink.redisMapper.OrderResultMapper;
import order.transformation.OrderProductTransaction;
import order.transformation.OrderTransaction;
import order.transformation.PojoTransaction;
import order.utils.PropertyUtils;
import order.utils.RocketMqConsumerUtils;
import order.utils.StreamEnv;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.List;

@Slf4j
public class DoubleElevenBigScreem {
    public static void main(String[] args) throws Exception {

        // TODO 1.env加载配置
        StreamExecutionEnvironment env = StreamEnv.setEnv(args);
        int timeOffset = PropertyUtils.getIntValue("window.time.offset");
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost(PropertyUtils.getStrValue("redis.host"))
                .setPort(PropertyUtils.getIntValue("redis.port"))
                .setDatabase(PropertyUtils.getIntValue("redis.database"))
                .setMaxTotal(PropertyUtils.getIntValue("redis.total"))
                .setTimeout(PropertyUtils.getIntValue("redis.timeout"))
                .build();
        // TODO 2.分表分流 (Order, OrderProduct, OrderReceive, orderJoinOrderReceive, orderJoinOrderProduct)
        //获取MQ的信息映射三元组
        SingleOutputStreamOperator<Tuple3<String, String, String>> sourceMap = RocketMqConsumerUtils.getRockerMqMessage(env);
        SingleOutputStreamOperator<Order> mapOrder = PojoTransaction.getOrderPojo(sourceMap);
        SingleOutputStreamOperator<OrderProduct> mapOrderProduct = PojoTransaction.getOrderProductPojo(sourceMap);
        //双流join
        DataStream<OrderProduct> orderJoinOrderProduct = OrderJoinOrderProduct.getOrderJoinOrderProduct(mapOrder, mapOrderProduct, args[0]);

        // TODO 3.业务计算
        //订单总GMV 总订单数
        DataStream<OrderResult> orderResult  = OrderTransaction.getOrderResult(mapOrder, timeOffset);
        //品牌的销售额 销量
        DataStream<OrderBrandResult> brandResult  = OrderProductTransaction.getOrderBrandResult(orderJoinOrderProduct, timeOffset);
        //品牌的销量top3
        DataStream<List<OrderBrandResult>> brandSalesTop5  = OrderProductTransaction.getOrderBrandSalesTop5(brandResult);

        // TODO 4.数据落地
        orderResult.addSink(new RedisSink<OrderResult>(config, new OrderResultMapper()));
        brandResult.addSink(new RedisSink<OrderBrandResult>(config, new OrderBrandResultMapper()));
        //top落地
        brandSalesTop5.addSink(new RedisSink<List<OrderBrandResult>>(config, new OrderBrandSalesTop5Mapper()));

        // TODO 5.触发执行
        try {
            env.execute("Flink Order");
        } catch (Exception e) {
            log.error("DoubleElevenBigScreem error", e);
        }
    }
}
