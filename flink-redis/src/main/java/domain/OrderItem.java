package domain;

import com.alibaba.fastjson.JSON;
import lombok.Data;


//订单明细类(订单id,商品id,商品数量)
@Data
public class OrderItem {
    private String itemId;
    private String goodsId;
    private Integer count;
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}