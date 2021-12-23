package watermark;

import domain.OrderItem;
import org.apache.flink.api.common.eventtime.*;

//构建水印分配器，学习测试直接使用系统时间了
public class OrderItemWatermark implements WatermarkStrategy<OrderItem> {

    @Override
    public TimestampAssigner<OrderItem> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (element, recordTimestamp) -> System.currentTimeMillis();
    }
    @Override
    public WatermarkGenerator<OrderItem> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<OrderItem>() {
            @Override
            public void onEvent(OrderItem event, long eventTimestamp, WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
        };
    }
}