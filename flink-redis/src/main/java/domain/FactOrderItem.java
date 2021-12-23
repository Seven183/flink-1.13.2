package domain;

import com.alibaba.fastjson.JSON;
import lombok.Data;

import java.math.BigDecimal;

//商品类(商品id,商品名称,商品价格)
//订单明细类(订单id,商品id,商品数量)
//关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
@Data
public class FactOrderItem {
    private String goodsId;
    private String goodsName;
    private BigDecimal count;
    private BigDecimal totalMoney;
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
