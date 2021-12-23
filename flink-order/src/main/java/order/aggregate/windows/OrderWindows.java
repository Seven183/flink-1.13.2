package order.aggregate.windows;

import order.result.OrderResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
   WindowFunction<IN, OUT, KEY, W extends Window>
 */
public class OrderWindows implements WindowFunction<OrderResult, OrderResult, Tuple2<String, String>, TimeWindow> {
    @Override
    public void apply(Tuple2<String, String> tuple2, TimeWindow timeWindow, Iterable<OrderResult> iterable, Collector<OrderResult> collector) throws Exception {

        OrderResult next = iterable.iterator().next();
        collector.collect(next);
    }
}
