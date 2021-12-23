package task.connector;

import aggregate.aggregate.PriceAggregate;
import aggregate.windows.WindowResult;
import domain.CategoryPojo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import process.KafkaWindowProcess;
import source.MySource;
import utils.EnvConfig;
import utils.KafkaConfig;
import utils.PropertyUtils;

import java.util.Properties;

public class FlinkCustomerToKafka {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.getEnvConfig(env);
        PropertyUtils.init(args[0]);

        //TODO 2.加载数据源
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource()).name("source");
        //TODO 3.transformation--初步聚合:每隔1s聚合一下截止到当前时间的各个分类的销售总金额
        DataStream<CategoryPojo> tempAggResult = orderDS
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .aggregate(new PriceAggregate(), new WindowResult())//aggregate支持复杂的自定义聚合
                .name("aggregate");

        //TODO 4.sink-kafka
        Properties kafkaConfig = KafkaConfig.getRedisConfig();
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>("flink_source", new SimpleStringSchema(), kafkaConfig);
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new KafkaWindowProcess())
                .addSink(kafkaSink)
                .name("process");

        //TODO 5.execute
        env.execute("kafka Test");
    }
}
