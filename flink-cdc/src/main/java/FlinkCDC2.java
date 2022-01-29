import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC2 {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("42.192.48.125")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test","test2")
                .tableList("test.name","test2.name2")
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDC");

    }
}
