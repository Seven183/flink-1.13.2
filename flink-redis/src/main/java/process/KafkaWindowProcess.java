package process;

import com.alibaba.fastjson.JSON;
import domain.CategoryPojo;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * 自定义窗口完成销售总额统计和分类销售额top3统计并输出
 * abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
 */
public class KafkaWindowProcess extends ProcessWindowFunction<CategoryPojo, String, String, TimeWindow> {

    @Override
    public void process(String dateTime, Context context, Iterable<CategoryPojo> elements, Collector<String> out) throws Exception {

        double total = 0D;//用来记录销售总额
        Queue<CategoryPojo> queue = new PriorityQueue<>(3,//初识容量
                (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);
        for (CategoryPojo element : elements) {
            double price = element.getTotalPrice();
            total += price;
            if(queue.size() < 3){
                queue.add(element);//或offer入队
            }else{
                if(price >= queue.peek().getTotalPrice()){//peek表示取出堆顶元素但不删除
                    //queue.remove(queue.peek());
                    queue.poll();//移除堆顶元素
                    queue.add(element);//或offer入队
                }
            }
        }

        List<CategoryPojo> top3List = queue.stream()
                .sorted((c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? -1 : 1)
                .collect(Collectors.toList());

        out.collect(JSON.toJSONString(top3List));
    }
}