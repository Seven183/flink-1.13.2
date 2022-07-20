import cc.blynk.clickhouse.BalancedClickhouseDataSource;

import javax.sql.DataSource;
import java.sql.*;


public class QueryClickhouse {
    private static Connection connection = null;

    static {
        try {
            DataSource dualDataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://42.192.48.125:8123,172.19.0.183:8123");
            Class.forName("cc.blynk.clickhouse.ClickHouseDriver");// 驱动包
//            String url = "jdbc:clickhouse://42.192.48.125:8123/default";
            String user = "default";
            String password = "123456";
//            connection = DriverManager.getConnection(url,user,password);
            connection = dualDataSource.getConnection(user,password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test.ttt");
//        System.out.println(resultSet.getString(1));
        ResultSetMetaData metaData = resultSet.getMetaData();
//        System.out.println(resultSet);
        int columnCount = metaData.getColumnCount();
//        System.out.println(columnCount);
        while (resultSet.next()){
            for (int i =1;i <= columnCount;i ++){
//                System.out.println(metaData.getColumnName(i) + ":" + resultSet.getString(i));
                String q = metaData.getColumnName(i);
                String q2 = resultSet.getString(i);
                System.out.println( q + ":" + q2);
            }
        }
    }
}
