package order.domain;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String TENANT_CODE   ;
    private String ORG_CODE   ;
    private String CHANNEL_CODE   ;
    private String ORDER_CODE   ;
    private String MEMBER_CODE   ;
    private String ORDER_TYPE   ;
    private String ORDER_STATUS   ;
    private BigDecimal AMOUNT   ;
    private BigDecimal PRODUCT_AMOUNT   ;
    private BigDecimal USED_POINT_AMOUNT   ;
    private String IS_OMNI_CHANNEL   ;
    private String CREATE_TIME   ;
    private String TERMINAL   ;
    private String IS_VALID   ;
}
