package order.sink.redisMapper;

import com.alibaba.fastjson.JSON;
import order.result.OrderBrandResult;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.List;
import java.util.Optional;

public class OrderBrandSalesTop5Mapper implements RedisMapper<List<OrderBrandResult>> {

    private static final String PREFIX_KEY = "Top";
    private static final Integer TTL = 60 * 60 * 24 * 7;

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, PREFIX_KEY);
    }

    @Override
    public Optional<String> getAdditionalKey(List<OrderBrandResult> orderBrandResult) {
        return Optional.of(PREFIX_KEY + ":" +
                orderBrandResult.get(0).getDt() + ":" +
                orderBrandResult.get(0).getTenantCode() + ":" +
                orderBrandResult.get(0).getIsOmniChannel());
    }

    @Override
    public Optional<Integer> getAdditionalTTL(List<OrderBrandResult> orderBrandResult) {
        return Optional.of(TTL);
    }

    @Override
    public String getKeyFromData(List<OrderBrandResult> orderBrandResult) {

        return  "BrandSalesTop5" ;
    }

    @Override
    public String getValueFromData(List<OrderBrandResult> orderBrandResult) {
        return JSON.toJSONString(orderBrandResult);
    }
}
