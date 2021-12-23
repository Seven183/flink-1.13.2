package order.aggregate.aggregate;

import lombok.SneakyThrows;
import order.domain.Order;
import order.result.OrderResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.math.BigDecimal;


public class OrderAggregate implements AggregateFunction<Order, OrderResult, OrderResult> {

    @Override
    public OrderResult createAccumulator() {
        return new OrderResult();
    }

    @SneakyThrows
    @Override
    public OrderResult add(Order order, OrderResult orderResult) {
        if (StringUtils.isNotBlank(orderResult.getTenantCode())){
            orderResult.setTenantCode(order.getTENANT_CODE());
            orderResult.setIsOmniChannel(order.getIS_OMNI_CHANNEL());
            orderResult.setOrderCount(orderResult.getOrderCount() + 1);
            orderResult.setOrderAmount(order.getAMOUNT().add(orderResult.getOrderAmount()));
//            orderResult.setOrderProductCount();
//            orderResult.setOrderMemberCount();
            orderResult.setDt(order.getCREATE_TIME().substring(0,10));
            orderResult.setLastTime(order.getCREATE_TIME());
            return orderResult;
        } else {
            orderResult.setTenantCode(order.getTENANT_CODE());
            orderResult.setIsOmniChannel(order.getIS_OMNI_CHANNEL());
            orderResult.setOrderCount(1L);
            orderResult.setOrderAmount(order.getAMOUNT());
//            orderResult.setOrderProductCount();
//            orderResult.setOrderMemberCount();
            orderResult.setDt(order.getCREATE_TIME().substring(0,10));
            orderResult.setLastTime(order.getCREATE_TIME());
            return orderResult;
        }
    }

    @Override
    public OrderResult getResult(OrderResult orderResult) {
        return orderResult;
    }

    @Override
    public OrderResult merge(OrderResult orderResult, OrderResult orderResult1) {
        Long count = orderResult.getOrderCount() + orderResult1.getOrderCount();
        BigDecimal add = orderResult.getOrderAmount().add(orderResult1.getOrderAmount());
        orderResult.setOrderAmount(add);
        orderResult.setOrderCount(count);
        return orderResult;
    }
}
