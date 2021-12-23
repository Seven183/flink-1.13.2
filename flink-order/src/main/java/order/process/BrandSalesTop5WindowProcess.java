package order.process;

import order.result.OrderBrandResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

public class BrandSalesTop5WindowProcess extends ProcessWindowFunction<OrderBrandResult, List<OrderBrandResult>, Tuple2<String, String>, TimeWindow> {

    @Override
    public void process(Tuple2<String, String> dateTime, Context context, Iterable<OrderBrandResult> elements, Collector<List<OrderBrandResult>> out) throws Exception {

        Queue<OrderBrandResult> queue = new PriorityQueue<>(5,//初识容量
                //正常的排序,就是小的在前,大的在后,也就是c1>c2的时候返回1,也就是升序,也就是小顶堆
                (c1, c2) -> c1.getOrderProductCount() >= c2.getOrderProductCount() ? 1 : -1);
        for (OrderBrandResult element : elements) {
            long count = element.getOrderProductCount();
            if(queue.size()< 5){
                queue.add(element);//或offer入队
            }else{
                if(count >= queue.peek().getOrderProductCount()){
                    queue.poll();//移除堆顶元素
                    queue.add(element);//或offer入队
                }
            }
        }

        List<OrderBrandResult> top5List = queue.stream()
                .sorted((c1, c2) -> c1.getOrderProductCount() >= c2.getOrderProductCount() ? -1 : 1)
                .collect(Collectors.toList());

        out.collect(top5List);
    }
}
