package order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Es implements Serializable {
    private String TENANT_CODE ;
    private String SKU_CODE ;
    private String SKU_NAME;
    private String BRAND_CODE;
    private String CATEGORY_CODE;
}
