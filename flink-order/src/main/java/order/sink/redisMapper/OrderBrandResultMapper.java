package order.sink.redisMapper;

import com.alibaba.fastjson.JSON;
import order.result.OrderBrandResult;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

public class OrderBrandResultMapper implements RedisMapper<OrderBrandResult> {

    private static final String PREFIX_KEY = "OrderBrand";
    private static final Integer TTL = 60 * 60 * 24 * 7;

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, PREFIX_KEY);
    }

    @Override
    public Optional<String> getAdditionalKey(OrderBrandResult orderBrandResult) {
        return Optional.of(PREFIX_KEY + ":" +
                orderBrandResult.getDt() + ":" +
                orderBrandResult.getTenantCode() + ":" +
                orderBrandResult.getIsOmniChannel());
    }

    @Override
    public Optional<Integer> getAdditionalTTL(OrderBrandResult orderBrandResult) {
        return Optional.of(TTL);
    }

    @Override
    public String getKeyFromData(OrderBrandResult orderBrandResult) {

        return "BRAND_CODE:" + orderBrandResult.getBrandCode();
    }

    @Override
    public String getValueFromData(OrderBrandResult orderBrandResult) {
        return JSON.toJSONString(orderBrandResult);
    }
}
