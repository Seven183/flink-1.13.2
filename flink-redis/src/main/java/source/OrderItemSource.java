package source;

import domain.Goods;
import domain.OrderItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

//实时生成订单数据流
//构建订单明细Stream源
public class OrderItemSource extends RichSourceFunction<OrderItem> {
    private Boolean isCancel;
    private Random r;
    @Override
    public void open(Configuration parameters) throws Exception {
        isCancel = false;
        r = new Random();
    }
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while(!isCancel) {
            Goods goods = Goods.randomGoods();
            OrderItem orderItem = new OrderItem();
            orderItem.setGoodsId(goods.getGoodsId());
            orderItem.setCount(r.nextInt(10) + 1);
            orderItem.setItemId(UUID.randomUUID().toString());
            sourceContext.collect(orderItem);
            orderItem.setGoodsId("111");
            sourceContext.collect(orderItem);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
