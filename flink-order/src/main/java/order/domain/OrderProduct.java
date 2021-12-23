package order.domain;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderProduct {
    private String TENANT_CODE ;
    private String ORG_CODE ;
    private String ORDER_CODE ;
    private String SKU_CODE ;
    private String PRODUCT_CODE ;
    private Long COUNT ;
    private String STATUS ;
    private BigDecimal USED_POINT_AMOUNT ;
    private BigDecimal AMOUNT ;
    private BigDecimal ORDER_AMOUNT ;
    private String CREATE_TIME ;
    private String IS_OMNI_CHANNEL;
    private String SKU_NAME;
    private String BRAND_CODE;
    private String CATEGORY_CODE;
}
