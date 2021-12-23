package utils;

import java.util.Properties;

public class KafkaConfig {

    public static Properties getRedisConfig (){
        Properties props  = new Properties();
        //集群地址
        props.setProperty("bootstrap.servers", PropertyUtils.getStrValue("kafka.sink.bootstrap.servers"));
        //消费者组id
        props.setProperty("group.id", PropertyUtils.getStrValue("kafka.sink.group.id"));
        //latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("auto.offset.reset",PropertyUtils.getStrValue("kafka.sink.auto.offset.reset"));
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("flink.partition-discovery.interval-millis",PropertyUtils.getStrValue("kafka.sink.flink.partition-discovery.interval-millis"));
        //自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("enable.auto.commit", PropertyUtils.getStrValue("kafka.sink.enable.auto.commit"));
        //自动提交的时间间隔
        props.setProperty("auto.commit.interval.ms", PropertyUtils.getStrValue("kafka.sink.auto.commit.interval.ms"));

        return props;
    }
}
