package map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class KafkaMap implements FlatMapFunction<String, Tuple3<String, String, String>> {

    @Override
    public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
        JSONArray array = JSONArray.parseArray(value);
        Iterator<Object> iterator = array.stream().iterator();
        while (iterator.hasNext()){
            String s = iterator.next().toString();
            JSONObject jsonObject = JSONObject.parseObject(s);
            String category = jsonObject.get("category").toString();
            String dateTime = jsonObject.get("dateTime").toString();
            String totalPrice = jsonObject.get("totalPrice").toString();
            out.collect(Tuple3.of(category,dateTime,totalPrice));
        }
    }
}
