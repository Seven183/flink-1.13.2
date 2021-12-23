package task.feature;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import source.MySource;
import utils.EnvConfig;
import utils.PropertyUtils;

import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Desc 演示Flink FileSink将批/流式数据写入到HDFS 数据一致性由Checkpoint + 两阶段提交保证
 */
public class FlinkSinkHDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.getEnvConfig(env);
        PropertyUtils.init(args[0]);

        String path ="hdfs://42.192.48.125:9870/FlinkFileSink/parquet";
        String path2 ="D:\\test\\out";
        //TODO 2.加载数据源
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource()).name("source");

        //TODO 3.sink
        orderDS.print();

        //使用FileSink将数据sink到HDFS
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        FileSink<Tuple2<String, Double>> sink = FileSink
                .forRowFormat(new Path(path2), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(config)
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
                .build();

        orderDS.sinkTo(sink);


        //TODO 4.execute
        env.execute();
    }
}
