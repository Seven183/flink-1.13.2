package order.transformation;

import com.alibaba.fastjson.JSONObject;
import order.domain.Order;
import order.domain.OrderProduct;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class PojoTransaction {

    public static SingleOutputStreamOperator<OrderProduct> getOrderProductPojo (SingleOutputStreamOperator<Tuple3<String, String, String>> sourceMap){
        SingleOutputStreamOperator<Tuple3<String, String, String>> sourceFilterOrder = sourceMap
                .filter(t -> t.f1.equals("t_order_product"));
        SingleOutputStreamOperator<OrderProduct> mapOrder = sourceFilterOrder.map(new MapFunction<Tuple3<String, String, String>, OrderProduct>() {
            @Override
            public OrderProduct map(Tuple3<String, String, String> tuple3) throws Exception {
                String f0 = tuple3.f0;
                return JSONObject.parseObject(f0, OrderProduct.class);
            }
        });
        SingleOutputStreamOperator<OrderProduct> dataSource = mapOrder.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderProduct>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((OrderProduct, timestamp) -> {
                    String create_time = OrderProduct.getCREATE_TIME();
                    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
                    Date date = null;
                    try {
                        date = sdf.parse(create_time);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return date.getTime();
                }));
        return dataSource;
    }

    public static SingleOutputStreamOperator<Order> getOrderPojo(SingleOutputStreamOperator<Tuple3<String, String, String>> sourceMap){
        SingleOutputStreamOperator<Tuple3<String, String, String>> sourceFilterOrder = sourceMap
                .filter(t -> t.f1.equals("t_order_order"));
        SingleOutputStreamOperator<Order> mapOrder = sourceFilterOrder.map(new MapFunction<Tuple3<String, String, String>, Order>() {
            @Override
            public Order map(Tuple3<String, String, String> tuple3) throws Exception {
                String f0 = tuple3.f0;
                return JSONObject.parseObject(f0, Order.class);
            }
        });

        // 处理乱序数据
        SingleOutputStreamOperator<Order> dataSource = mapOrder.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((order, timestamp) -> {
                    String create_time = order.getCREATE_TIME();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = null;
                    try {
                        date = sdf.parse(create_time);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return date.getTime();
                }));
        return dataSource;
    }
}
