package task.connector;

import aggregate.aggregate.PriceAggregate;
import aggregate.windows.WindowResult;
import domain.CategoryPojo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import process.RedisWindowProcess;
import sink.redisMapper.CategoryPojoMapper;
import source.MySource;
import utils.EnvConfig;
import utils.PropertyUtils;
import utils.RedisConfig;

import java.util.List;

public class FlinkCustomerToRedis {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.getEnvConfig(env);
        PropertyUtils.init(args[0]);

        //TODO 2.加载数据源
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource()).name("source");

        //TODO 3.transformation
        DataStream<CategoryPojo> tempAggResult = orderDS
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(30)))
                .aggregate(new PriceAggregate(), new WindowResult())
                .name("aggregate");//aggregate支持复杂的自定义聚合

        //TODO 4.sink-redis
        FlinkJedisPoolConfig config = RedisConfig.getRedisConfig();
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new RedisWindowProcess())
                .addSink(new RedisSink<List<CategoryPojo>>(config, new CategoryPojoMapper()))
                .name("process");

        //TODO 5.execute
        env.execute("Redis Test");
    }
}
