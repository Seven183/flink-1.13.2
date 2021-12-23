package order.utils;

import order.map.RocketMqSourceMap;
import order.source.SimpleStringDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.legacy.RocketMQSourceFunction;

import java.util.Properties;

/**
 * 读取rocketMq的消息映射三元组Tuple3<demo, table, type> 分表分流
 */
public class RocketMqConsumerUtils {

    public static SingleOutputStreamOperator<Tuple3<String, String, String>> getRockerMqMessage( StreamExecutionEnvironment env){
        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, PropertyUtils.getStrValue("rocket.nameSrvAddr"));
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, PropertyUtils.getStrValue("rocket.consumer.group"));
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, PropertyUtils.getStrValue("rocket.topic"));
        DataStreamSource<String> dataStreamSource = env.addSource(new RocketMQSourceFunction<String>(new SimpleStringDeserializationSchema(), consumerProps));
        SingleOutputStreamOperator<Tuple3<String, String, String>> sourceMap = dataStreamSource.flatMap(new RocketMqSourceMap());
        return sourceMap;
    }
}
