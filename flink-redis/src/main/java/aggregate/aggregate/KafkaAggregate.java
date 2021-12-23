package aggregate.aggregate;

import domain.CategoryPojo;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义聚合函数,指定聚合规则
 * AggregateFunction<IN, ACC, OUT>
 */
public class KafkaAggregate implements AggregateFunction<CategoryPojo, List<CategoryPojo>, List<CategoryPojo>> {

    @Override
    public List<CategoryPojo> createAccumulator() {
        return new ArrayList<CategoryPojo>();
    }

    @Override
    public List<CategoryPojo> add(CategoryPojo value, List<CategoryPojo> accumulator) {

        for (CategoryPojo next : accumulator) {
            if (value.getCategory().equals(next.getCategory())) {
                next.setTotalPrice(next.getTotalPrice() + value.getTotalPrice());
                next.setDateTime(value.getDateTime());
                break;
            }
        }
        if (!accumulator.toString().contains(value.getCategory())) {
            accumulator.add(value);
        }
        return accumulator;
    }

    @Override
    public List<CategoryPojo> getResult(List<CategoryPojo> accumulator) {
        return accumulator;
    }

    @Override
    public List<CategoryPojo> merge(List<CategoryPojo> a, List<CategoryPojo> b) {

        a.addAll(b);
        Map<String, CategoryPojo> map = new HashMap<>();
        a.forEach(categoryPojo -> {
            CategoryPojo last = map.get(categoryPojo.getCategory());
            if(null != last){
                categoryPojo.setTotalPrice(categoryPojo.getTotalPrice() + last.getTotalPrice());
            }
            map.put(categoryPojo.getCategory(), categoryPojo );
        });
        return new ArrayList<>(map.values());
    }
}