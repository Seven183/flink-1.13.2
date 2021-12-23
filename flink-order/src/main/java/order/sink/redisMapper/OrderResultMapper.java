package order.sink.redisMapper;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import order.result.OrderResult;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

@Slf4j
public class OrderResultMapper implements RedisMapper<OrderResult> {

    private static final String PREFIX_KEY = "Order";
    private static final Integer TTL = 60 * 60 * 24 * 7;

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, PREFIX_KEY);
    }

    @Override
    public Optional<String> getAdditionalKey(OrderResult orderResult) {
        return Optional.of(PREFIX_KEY + ":" +
                orderResult.getDt() + ":" +
                orderResult.getTenantCode() + ":" +
                orderResult.getIsOmniChannel());
    }

    @Override
    public Optional<Integer> getAdditionalTTL(OrderResult orderResult) {
        return Optional.of(TTL);
    }

    @Override
    public String getKeyFromData(OrderResult orderResult) {
        return "TENANT_CODE:" + orderResult.getTenantCode() + ":"
                + "IS_OMNI_CHANNEL:" + orderResult.getIsOmniChannel() + ":"
                + "DT:" + orderResult.getDt();
    }

    @Override
    public String getValueFromData(OrderResult orderResult) {
        return JSON.toJSONString(orderResult);
    }
}
