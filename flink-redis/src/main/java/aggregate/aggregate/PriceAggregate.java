package aggregate.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义聚合函数,指定聚合规则
 * AggregateFunction<IN, ACC, OUT>
 */
public class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
    //初始化累加器
    @Override
    public Double createAccumulator() {
        return 0D;//D表示double,L表示Long
    }

    //把数据累加到累加器上
    @Override
    public Double add(Tuple2<String, Double> value, Double accumulator) {
        return value.f1 + accumulator;
    }

    //获取累加结果
    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    //合并各个subtask的结果
    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}