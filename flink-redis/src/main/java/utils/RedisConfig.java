package utils;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisConfig {

    public static FlinkJedisPoolConfig getRedisConfig (){
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost(PropertyUtils.getStrValue("redis.host"))
                .setPort(PropertyUtils.getIntValue("redis.port"))
                .setPassword(PropertyUtils.getStrValue("redis.passwd"))
                .setDatabase(PropertyUtils.getIntValue("redis.database"))
                .setMaxTotal(PropertyUtils.getIntValue("redis.total"))
                .setTimeout(PropertyUtils.getIntValue("redis.timeout"))
                .build();

        return config;
    }
}
