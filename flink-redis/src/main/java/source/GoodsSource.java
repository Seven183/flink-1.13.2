package source;

import domain.Goods;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;


//实时生成商品数据流
//构建一个商品Stream源（这个好比就是维表）
public class GoodsSource extends RichSourceFunction<Goods> {

    private Boolean isCancel;
    @Override
    public void open(Configuration parameters) throws Exception {
        isCancel = false;
    }
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while(!isCancel) {
            Goods.GOODS_LIST.stream().forEach(goods -> sourceContext.collect(goods));
            TimeUnit.SECONDS.sleep(1);
        }
    }
    @Override
    public void cancel() {
        isCancel = true;
    }
}