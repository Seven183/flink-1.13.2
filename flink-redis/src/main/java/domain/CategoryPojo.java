package domain;

/**
 * 用于存储聚合的结果
 */
public class CategoryPojo {
    private String category;//分类名称
    private double totalPrice;//该分类总销售额
    private String dateTime;// 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可

    public CategoryPojo(String category, double totalPrice, String dateTime) {
        this.category = category;
        this.totalPrice = totalPrice;
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "CategoryPojo{" +
                "category='" + category + '\'' +
                ", totalPrice=" + totalPrice +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }
}
