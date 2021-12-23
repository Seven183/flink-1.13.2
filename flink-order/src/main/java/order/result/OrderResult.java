package order.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderResult {
    private String tenantCode;
    private String isOmniChannel;
    private Long orderCount;
    private BigDecimal orderAmount;
    private Long orderProductCount;
    private Long orderMemberCount;
    private String dt;
    private String lastTime;
}
