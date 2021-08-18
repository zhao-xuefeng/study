package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.TimeZone;

public class TrinoJDBCUtil {
    private static String url = "jdbc:trino://hebing4:8433/hive";
    private static Connection conn = null;

    private static void initConnection(String schemaName) {
        // 设置时区
        TimeZone.setDefault(TimeZone.getTimeZone("+08:00"));
        if (conn == null) {
            try {
                Class.forName("io.trino.jdbc.TrinoDriver");
                url += "/" + schemaName;
                Properties properties = new Properties();
                properties.setProperty("user", "trino2");
                properties.setProperty("password", "bonctrino");
                properties.setProperty("SSL", "true");
                properties.setProperty("SSLKeyStorePath", "D:\\private\\trino.jks");
                properties.setProperty("SSLKeyStorePassword", "bonctrino");
                conn = DriverManager.getConnection(url, properties);
                ResultSet set=conn.prepareStatement("select * from test.test").executeQuery();
                while (set.next()){
                    System.out.println(set.getString(1)+"\t"+set.getString(2));
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("获取Trino连接异常");
            }
        }
    }

    public static void main(String[] args) {
        initConnection("test");
    }
}