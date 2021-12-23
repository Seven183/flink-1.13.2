package order.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class RocketMqSourceMap implements FlatMapFunction<String, Tuple3<String, String, String>> {

    @Override
    public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String table = jsonObject.get("table").toString();
        String type = jsonObject.get("type").toString();
        if (type.equals("INSERT")){
            JSONArray jsonArray = JSONObject.parseArray(jsonObject.get("data").toString());
            Iterator<Object> iterator = jsonArray.iterator();
            while (iterator.hasNext()){
                String item = JSONObject.parseObject(iterator.next().toString()).toString();
                out.collect(Tuple3.of(item,table,table));
            }
        }
    }
}
