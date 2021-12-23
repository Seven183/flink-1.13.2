package sink.redisMapper;

import com.alibaba.fastjson.JSON;
import domain.CategoryPojo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Slf4j
public class CategoryPojoMapper implements RedisMapper<List<CategoryPojo>> {

    private static final String PREFIX_KEY = "Category";
    private static final Integer TTL = 60 * 60 * 24 * 7;

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, PREFIX_KEY);
    }

    @Override
    public Optional<String> getAdditionalKey(List<CategoryPojo> categoryPojo) {
        return Optional.of(PREFIX_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    }

    @Override
    public Optional<Integer> getAdditionalTTL(List<CategoryPojo> orderResult) {
        return Optional.of(TTL);
    }

    @Override
    public String getKeyFromData(List<CategoryPojo> categoryPojo) {
        return "top3";
    }

    @Override
    public String getValueFromData(List<CategoryPojo> categoryPojo) {
        return JSON.toJSONString(categoryPojo);
    }
}
