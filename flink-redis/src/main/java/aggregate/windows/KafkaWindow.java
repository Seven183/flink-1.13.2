package aggregate.windows;

import com.alibaba.fastjson.JSON;
import domain.CategoryPojo;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 自定义窗口函数,指定窗口数据收集规则
 * WindowFunction<IN, OUT, KEY, W extends Window>
 */
public class KafkaWindow implements WindowFunction<List<CategoryPojo>, String, String, TimeWindow> {

    @Override
    public void apply(String category, TimeWindow window, Iterable<List<CategoryPojo>> input, Collector<String> out) throws Exception {
        out.collect(JSON.toJSONString(input));
    }
}