package gbaseTrain;

import java.sql.*;

public class GbaseConnect {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    public static Connection connectGBase(){
        try {

            Class.forName("com.gbase.jdbc.Driver");
            String url="jdbc:gbase://10.130.7.228:5258/protest";
            String username="root";
            String password="gtest321";
            conn=DriverManager.getConnection(url,username,password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }
    public static void closeGbase()  {
        if (rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt!=null){
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
    public static ResultSet query(String sql) throws SQLException {
        conn= connectGBase();
        pstmt=conn.prepareStatement(sql);
        rs=pstmt.executeQuery();
        return rs;
    }



}
