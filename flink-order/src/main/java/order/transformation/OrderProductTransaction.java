package order.transformation;

import order.aggregate.aggregate.OrderBrandAggregate;
import order.aggregate.windows.OrderBrandWindows;
import order.domain.OrderProduct;
import order.process.BrandSalesTop5WindowProcess;
import order.result.OrderBrandResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.util.List;

public class OrderProductTransaction {

   //计算品牌
    public static DataStream<OrderBrandResult> getOrderBrandResult(DataStream<OrderProduct> orderJoinOrderProduct, int timeOffset){
        SingleOutputStreamOperator<OrderBrandResult> orderProduct = orderJoinOrderProduct
                .filter(t -> StringUtils.isNotBlank(t.getBRAND_CODE()))
                .keyBy(new KeySelector<OrderProduct, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(OrderProduct orderProduct) throws Exception {
                        return Tuple3.of(orderProduct.getTENANT_CODE(), orderProduct.getBRAND_CODE(), orderProduct.getIS_OMNI_CHANNEL());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-timeOffset)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .aggregate(new OrderBrandAggregate(), new OrderBrandWindows());

        return orderProduct;
    }

    public static DataStream<List<OrderBrandResult>> getOrderBrandSalesTop5(DataStream<OrderBrandResult> orderBrandResult){

        DataStream<List<OrderBrandResult>> process = orderBrandResult
                .keyBy(new KeySelector<OrderBrandResult, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(OrderBrandResult orderProduct) throws Exception {
                        return Tuple2.of(orderProduct.getTenantCode(), orderProduct.getIsOmniChannel());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new BrandSalesTop5WindowProcess());

        return process;
    }
}
