package order.transformation;

import order.aggregate.aggregate.OrderAggregate;
import order.aggregate.windows.OrderWindows;
import order.domain.Order;
import order.result.OrderResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;


public class OrderTransaction {

    public static SingleOutputStreamOperator<OrderResult> getOrderResult(SingleOutputStreamOperator<Order> mapOrder, int timeOffset){
        SingleOutputStreamOperator<OrderResult> aggregate = mapOrder
                .keyBy(new KeySelector<Order, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Order order) throws Exception {
                        return Tuple2.of(order.getTENANT_CODE(), order.getIS_OMNI_CHANNEL());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-timeOffset)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .aggregate(new OrderAggregate(), new OrderWindows());

        return aggregate;
    }
}
