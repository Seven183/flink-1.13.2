package domain;

import java.io.Serializable;

public class Es implements Serializable {
    private String TENANT_CODE ;
    private String SKU_CODE ;
    private String SKU_NAME;
    private String BRAND_CODE;
    private String CATEGORY_CODE;

    @Override
    public String toString() {
        return "Es{" +
                "TENANT_CODE='" + TENANT_CODE + '\'' +
                ", SKU_CODE='" + SKU_CODE + '\'' +
                ", SKU_NAME='" + SKU_NAME + '\'' +
                ", BRAND_CODE='" + BRAND_CODE + '\'' +
                ", CATEGORY_CODE='" + CATEGORY_CODE + '\'' +
                '}';
    }

    public String getTENANT_CODE() {
        return TENANT_CODE;
    }

    public void setTENANT_CODE(String TENANT_CODE) {
        this.TENANT_CODE = TENANT_CODE;
    }

    public String getSKU_CODE() {
        return SKU_CODE;
    }

    public void setSKU_CODE(String SKU_CODE) {
        this.SKU_CODE = SKU_CODE;
    }

    public String getSKU_NAME() {
        return SKU_NAME;
    }

    public void setSKU_NAME(String SKU_NAME) {
        this.SKU_NAME = SKU_NAME;
    }

    public String getBRAND_CODE() {
        return BRAND_CODE;
    }

    public void setBRAND_CODE(String BRAND_CODE) {
        this.BRAND_CODE = BRAND_CODE;
    }

    public String getCATEGORY_CODE() {
        return CATEGORY_CODE;
    }

    public void setCATEGORY_CODE(String CATEGORY_CODE) {
        this.CATEGORY_CODE = CATEGORY_CODE;
    }
}
