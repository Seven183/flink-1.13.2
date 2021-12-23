package order.join;

import order.domain.Es;
import order.domain.Order;
import order.domain.OrderProduct;
import order.utils.EsQuery;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *
 * order orderProduct 双流join
 * 获取es中的维度信息
 */
public class OrderJoinOrderProduct {
    public static DataStream<OrderProduct> getOrderJoinOrderProduct(SingleOutputStreamOperator<Order> mapOrder,
                                                                    SingleOutputStreamOperator<OrderProduct> mapOrderProduct, String envP) {
        DataStream<OrderProduct> apply = mapOrder
            .join(mapOrderProduct)
            .where(Order::getORDER_CODE)
            .equalTo(OrderProduct::getORDER_CODE)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new JoinFunction<Order, OrderProduct, OrderProduct>() {
                @Override
                public OrderProduct join(Order order, OrderProduct orderProduct) throws Exception {

                    Es es = EsQuery.getEs(orderProduct.getTENANT_CODE(), orderProduct.getSKU_CODE(), envP);

                    String sku_name = es.getSKU_NAME();
                    String brand_code = es.getBRAND_CODE();
                    String category_code = es.getCATEGORY_CODE();
                    OrderProduct orderProduct1 = new OrderProduct();

                    if (StringUtils.isBlank(sku_name)) {
                        orderProduct1.setSKU_NAME(orderProduct.getSKU_NAME());
                    } else {
                        orderProduct1.setSKU_NAME(es.getSKU_NAME());
                    }

                    if (StringUtils.isBlank(brand_code)) {
                        orderProduct1.setBRAND_CODE(orderProduct.getBRAND_CODE());
                    } else {
                        orderProduct1.setBRAND_CODE(es.getBRAND_CODE());
                    }

                    if (StringUtils.isBlank(category_code)) {
                        orderProduct1.setCATEGORY_CODE(orderProduct.getCATEGORY_CODE());
                    } else {
                        orderProduct1.setCATEGORY_CODE(es.getCATEGORY_CODE());
                    }

                    orderProduct1.setTENANT_CODE(order.getTENANT_CODE());
                    orderProduct1.setORG_CODE(order.getORG_CODE());
                    orderProduct1.setORDER_CODE(order.getORDER_CODE());
                    orderProduct1.setSKU_CODE(orderProduct.getSKU_CODE());
                    orderProduct1.setPRODUCT_CODE(orderProduct.getPRODUCT_CODE());
                    orderProduct1.setCOUNT(orderProduct.getCOUNT());
                    orderProduct1.setSTATUS(orderProduct.getSTATUS());
                    orderProduct1.setUSED_POINT_AMOUNT(orderProduct.getUSED_POINT_AMOUNT());
                    orderProduct1.setAMOUNT(orderProduct.getAMOUNT());
                    orderProduct1.setORDER_AMOUNT(order.getAMOUNT());
                    orderProduct1.setCREATE_TIME(orderProduct.getCREATE_TIME());
                    orderProduct1.setIS_OMNI_CHANNEL(order.getIS_OMNI_CHANNEL());
                    return orderProduct1;
                }
            });
        return apply;
    }
}
