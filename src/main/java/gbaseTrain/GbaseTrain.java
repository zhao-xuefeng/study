package gbaseTrain;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GbaseTrain {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;

    public static void queryTable(){
        conn= GbaseConnect.connectGBase();
        try {
            rs= GbaseConnect.query("select table_name from information_schema.tables where table_schema='protest' ");
            while (rs.next()){
                System.out.println(rs.getString("table_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            GbaseConnect.closeGbase();
        }
    }
    public static void queryTableData(String tableName,String databaseName){
        conn= GbaseConnect.connectGBase();
        try {
            rs= GbaseConnect.query("select column_name,column_comment,data_type from information_schema.columns where table_name='"+tableName+"\'"+"and table_schema='"+databaseName+"\'");
            while(rs.next()){
                System.out.println(rs.getString("column_name")+"    "
                        +rs.getString("column_comment")+"   "+rs.getString("data_type"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            GbaseConnect.closeGbase();
        }

    }

    public static void queryDataByTable()  {
        conn= GbaseConnect.connectGBase();
        String sql="select count(*) as f from aaa ";
        try {
            rs= GbaseConnect.query(sql);
            while(rs.next()){
                System.out.println(rs.getString("f"));

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            GbaseConnect.closeGbase();
        }
    }

    public static void createTable(String sql){
        conn= GbaseConnect.connectGBase();
        try {
            pstmt=conn.prepareStatement(sql);
            boolean flag=pstmt.execute();
            System.out.println(flag);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            GbaseConnect.closeGbase();
        }
    }

    public static void dataExport(String tableName,String path,String split){
        String  sql="select * from "+tableName+" into outfile "+"\'"+path+"\'"+" fields terminated by "+"\'"+split+"" +
                "\';";
//        String sql2="select * from aaa";
        System.out.println(sql);
        conn= GbaseConnect.connectGBase();
        try {
            pstmt=conn.prepareStatement(sql);
            Boolean flag=pstmt.execute();
            System.out.println(flag);
            GbaseConnect.closeGbase();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {
        queryTable();
//        queryTableData("aaa","protest");
//        queryDataByTable();
//        dataExport("aaa","/data/gbaseTestaaa.csv",",");
    }

}
