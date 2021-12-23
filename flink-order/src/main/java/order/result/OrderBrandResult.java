package order.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderBrandResult {
    private String tenantCode;
    private String brandCode;
    private String isOmniChannel;
    private Long orderCount;
    private BigDecimal orderAmount;
    private Long orderProductCount;
    private Long orderMemberCount;
    private String dt;
    private String lastTime;
}
