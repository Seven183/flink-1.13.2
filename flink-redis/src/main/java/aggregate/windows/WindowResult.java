package aggregate.windows;

import domain.CategoryPojo;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 自定义窗口函数,指定窗口数据收集规则
 * WindowFunction<IN, OUT, KEY, W extends Window>
 */
public class WindowResult implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
    private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    @Override
    //void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out)
    public void apply(String category, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
        long currentTimeMillis = System.currentTimeMillis();
        String dateTime = df.format(currentTimeMillis);
        Double totalPrice = input.iterator().next();
        out.collect(new CategoryPojo(category,totalPrice,dateTime));
    }
}