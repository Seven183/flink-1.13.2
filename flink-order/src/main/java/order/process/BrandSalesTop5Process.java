package order.process;


import order.result.OrderBrandResult;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

public class BrandSalesTop5Process extends KeyedProcessFunction< Long, OrderBrandResult, List<OrderBrandResult>> {

    private ListState<OrderBrandResult> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<OrderBrandResult> itemViewStateDesc = new ListStateDescriptor<OrderBrandResult>("BrandSalesTop5Process", OrderBrandResult.class);
        itemState = getRuntimeContext().getListState(itemViewStateDesc);
    }

    @Override
    public void processElement(OrderBrandResult orderBrandResult, Context context, Collector<List<OrderBrandResult>> collector) throws Exception {
        itemState.add(orderBrandResult);
        Queue<OrderBrandResult> queue = new PriorityQueue<>(5,//初识容量
                //正常的排序,就是小的在前,大的在后,也就是c1>c2的时候返回1,也就是升序,也就是小顶堆
                (c1, c2) -> c1.getOrderProductCount() >= c2.getOrderProductCount() ? 1 : -1);

        for (OrderBrandResult OrderBran: itemState.get()){
            long count = OrderBran.getOrderProductCount();
            if(queue.size()< 5){
                queue.add(OrderBran);//或offer入队
            }else{
                if(count >= queue.peek().getOrderProductCount()){
                    queue.poll();//移除堆顶元素
                    queue.add(OrderBran);//或offer入队
                }
            }
        }

        List<OrderBrandResult> top5List = queue.stream()
                .sorted((c1, c2) -> c1.getOrderProductCount() >= c2.getOrderProductCount() ? -1 : 1)
                .collect(Collectors.toList());

        collector.collect(top5List);
    }
}
