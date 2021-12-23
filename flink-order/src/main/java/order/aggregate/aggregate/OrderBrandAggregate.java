package order.aggregate.aggregate;

import order.domain.OrderProduct;
import order.result.OrderBrandResult;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.math.BigDecimal;

public class OrderBrandAggregate implements AggregateFunction<OrderProduct, OrderBrandResult, OrderBrandResult> {

    @Override
    public OrderBrandResult createAccumulator() {
        return new OrderBrandResult();
    }

    @Override
    public OrderBrandResult add(OrderProduct orderProduct, OrderBrandResult orderBrandResult) {
        if (StringUtils.isNotBlank(orderBrandResult.getTenantCode())){
            orderBrandResult.setTenantCode(orderProduct.getTENANT_CODE());
            orderBrandResult.setBrandCode(orderProduct.getBRAND_CODE());
            orderBrandResult.setIsOmniChannel(orderProduct.getIS_OMNI_CHANNEL());
//            orderBrandResult.setOrderCount(orderBrandResult.getOrderProductCount() + orderProduct.getCOUNT());
            orderBrandResult.setOrderAmount(orderBrandResult.getOrderAmount().add(orderProduct.getAMOUNT()));
            orderBrandResult.setOrderProductCount(orderBrandResult.getOrderProductCount() + orderProduct.getCOUNT());
//            orderBrandResult.setOrderMemberCount();
            orderBrandResult.setDt(orderProduct.getCREATE_TIME().substring(0,10));
            orderBrandResult.setLastTime(orderProduct.getCREATE_TIME());
            return orderBrandResult;
        } else {
            orderBrandResult.setTenantCode(orderProduct.getTENANT_CODE());
            orderBrandResult.setBrandCode(orderProduct.getBRAND_CODE());
            orderBrandResult.setIsOmniChannel(orderProduct.getIS_OMNI_CHANNEL());
//            orderBrandResult.setOrderCount(orderProduct.getCOUNT());
            orderBrandResult.setOrderAmount(orderProduct.getAMOUNT());
            orderBrandResult.setOrderProductCount(orderProduct.getCOUNT());
//            orderBrandResult.setOrderMemberCount();
            orderBrandResult.setDt(orderProduct.getCREATE_TIME().substring(0,10));
            orderBrandResult.setLastTime(orderProduct.getCREATE_TIME());
            return orderBrandResult;
        }
    }

    @Override
    public OrderBrandResult getResult(OrderBrandResult orderBrandResult) {
        return orderBrandResult;
    }

    @Override
    public OrderBrandResult merge(OrderBrandResult orderBrandResult, OrderBrandResult orderBrandResult1) {
        BigDecimal add = orderBrandResult.getOrderAmount().add(orderBrandResult1.getOrderAmount());
        Long count = orderBrandResult.getOrderProductCount() + orderBrandResult.getOrderProductCount();
        orderBrandResult.setOrderProductCount(count);
        orderBrandResult.setOrderAmount(add);
        return orderBrandResult;
    }
}
