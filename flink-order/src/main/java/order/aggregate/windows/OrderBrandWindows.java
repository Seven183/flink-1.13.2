package order.aggregate.windows;

import order.result.OrderBrandResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
   WindowFunction<IN, OUT, KEY, W extends Window>
 */
public class OrderBrandWindows implements WindowFunction<OrderBrandResult, OrderBrandResult, Tuple3<String,String,String>, TimeWindow> {
    @Override
    public void apply(Tuple3<String,String,String> tuple2, TimeWindow timeWindow, Iterable<OrderBrandResult> iterable, Collector<OrderBrandResult> collector) throws Exception {

        OrderBrandResult next = iterable.iterator().next();
        collector.collect(next);
    }
}
