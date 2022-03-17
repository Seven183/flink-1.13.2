import cc.blynk.clickhouse.copy.CopyManager;
import cc.blynk.clickhouse.copy.CopyManagerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;

public class QueryClickhouse3 {

    public static void main(String[] args) throws Exception {
        String driver = "cc.blynk.clickhouse.ClickHouseDriver";
        String ip = "42.192.48.125";
        String port = "8123";
        String db = "default";
        String user = "default";
        String pwd = "123456";
        // 数据输出文件
        String fileName = "t_order_mt.csv";
        Class.forName(driver);
        String urlSb = "jdbc:clickhouse://" + ip + ":" + port + "/" + db;
        Connection connection = DriverManager.getConnection(urlSb, user, pwd);

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String query = "select id,sku_id,total_amount,create_time from default.t_order_mt FORMAT CSVWithNames";
        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName));
        copyManager.copyFromDb(query, fileOutputStream);

        fileOutputStream.flush();
        fileOutputStream.close();
        copyManager.close();
        connection.close();

    }
}
