package source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义数据源实时产生订单数据Tuple2<分类, 金额>
 */
public class MySource implements SourceFunction<Tuple2<String, Double>> {
    private boolean flag = true;
    private String[] categorys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};
    private Random random = new Random();

    @Override
    public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
        while (flag) {
            //随机生成分类和金额
            int index = random.nextInt(categorys.length);//[0~length) ==> [0~length-1]
            String category = categorys[index];//获取的随机分类
            double price = random.nextDouble() ;//注意nextDouble生成的是[0~1)之间的随机小数,*100之后表示[0~100)的随机小数
            ctx.collect(Tuple2.of(category, price));
            Thread.sleep(60000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}