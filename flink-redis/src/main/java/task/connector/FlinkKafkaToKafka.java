package task.connector;

import com.alibaba.fastjson.JSON;
import domain.CategoryPojo;
import map.KafkaMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import utils.EnvConfig;
import utils.KafkaConfig;
import utils.PropertyUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkKafkaToKafka {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.getEnvConfig(env);
        PropertyUtils.init(args[0]);

        //TODO 2.加载数据源
        Properties kafkaConfig = KafkaConfig.getRedisConfig();
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink_source", new SimpleStringSchema(), kafkaConfig);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource);


        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>("flink_sink", new SimpleStringSchema(), kafkaConfig);

        //TODO 3.transformation--初步聚合:每隔1s聚合一下截止到当前时间的各个分类的销售总金额
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
            .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
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
            .addSink(kafkaSink);

        //TODO 5.execute
        env.execute("kafka Test");
    }
}
