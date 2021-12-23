package task.dataStream;

import aggregate.aggregate.PriceAggregate;
import aggregate.windows.WindowResult;
import com.alibaba.fastjson.JSON;
import domain.CategoryPojo;
import map.KafkaMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import process.KafkaWindowProcess;
import process.RedisWindowProcess;
import sink.redisMapper.CategoryPojoMapper;
import source.MySource;
import utils.EnvConfig;
import utils.KafkaConfig;
import utils.PropertyUtils;
import utils.RedisConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class FlinkDataStream {
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
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(60)))
                .aggregate(new PriceAggregate(), new WindowResult())
                .name("aggregate");//aggregate支持复杂的自定义聚合


        //TODO 4.sink-redis
        FlinkJedisPoolConfig config = RedisConfig.getRedisConfig();
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .process(new RedisWindowProcess())
                .addSink(new RedisSink<List<CategoryPojo>>(config, new CategoryPojoMapper()))
                .name("redisProcess");


        //TODO 4.sink-kafka
        Properties kafkaConfig = KafkaConfig.getRedisConfig();
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>("flink_source", new SimpleStringSchema(), kafkaConfig);
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .process(new KafkaWindowProcess())
                .addSink(kafkaSink)
                .name("kafkaProcess");


        //TODO 4.获取Kafka的数据, 处理后在返回Kafka
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink_source", new SimpleStringSchema(), kafkaConfig);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource);

        FlinkKafkaProducer<String> kafkaSink2 = new FlinkKafkaProducer<String>("flink_sink", new SimpleStringSchema(), kafkaConfig);
        stringDataStreamSource
            .flatMap(new KafkaMap())
            .map(t -> new CategoryPojo(t.f0, Double.parseDouble(t.f2), t.f1))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<CategoryPojo>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((categoryPojo, timestamp) -> {
                        try {
                            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(categoryPojo.getDateTime()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return timestamp;
                    }))
            .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
            .trigger(ContinuousEventTimeTrigger.of(Time.seconds(60)))
            .process(new ProcessAllWindowFunction<CategoryPojo, String, TimeWindow>() {
                @Override
                public void process(Context context, Iterable<CategoryPojo> iterable, Collector<String> collector) throws Exception {
                    Map<String, CategoryPojo> map = new HashMap<>();
                    iterable.forEach(categoryPojo -> {
                        CategoryPojo last = map.get(categoryPojo.getCategory());
                        if(null != last){
                            categoryPojo.setTotalPrice(categoryPojo.getTotalPrice() + last.getTotalPrice());
                        }
                        map.put(categoryPojo.getCategory(), categoryPojo );
                    });
                    collector.collect(JSON.toJSONString(new ArrayList<>(map.values())));
                }
            })
            .addSink(kafkaSink2)
            .name("kafkaToKafka");

        //TODO 5.execute
        env.execute("Redis Test");
    }
}
