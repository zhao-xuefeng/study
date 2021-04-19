package csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.sql.*;

public class CommonCsv {
    public static void main(String[] args) throws IOException, SQLException {

//        readByAuto("E:\\studyWorkSpace\\ceshi\\src\\main\\resources\\datafile\\a.csv");
        wirteToHdfs();
    }


    /**
     * 按照自己定义的数组表头读取
     *
     * @param path
     * @throws IOException
     */
    public static void readByArr(String path) throws IOException {
        Reader in = new FileReader(path);
        String[] header = {"id", "name"};
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(header).parse(in);
        for (CSVRecord record : records) {
            String lastName = record.get("id");
            String firstName = record.get("name");
            System.out.println(lastName);
            System.out.println(firstName);
        }
    }

    /**
     * 按照csv的第一行读取
     *
     * @param path
     * @throws IOException
     */
    public static void readByAuto(String path) throws IOException {
        Reader in = new FileReader(path);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            String lastName = record.get("id");
            String firstName = record.get("name");
            System.out.println(lastName);
            System.out.println(firstName);
        }
    }


    /**
     * 将JDBC读取的数据写CSV到HDFS
     *
     * @throws IOException
     */
    public static void wirteToHdfs() throws IOException, SQLException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);
        FSDataOutputStream fos = fs.create(new Path("/user/zgh/cdabcdhaj"));
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fos);
        CSVPrinter printer = null;
//                new CSVPrinter(new OutputStreamWriter(fos), CSVFormat.DEFAULT.withHeader());
//这里是连接数据库
        Connection connection = DriverManager.getConnection("ada","ada","dada");
        Statement statement = null;;
        ResultSet resultSet=null;
        //获得表头信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        String[] cloumns=new String[metaData.getColumnCount()];
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            cloumns[i-1]=(resultSet.getMetaData().getColumnName(i));
        }
        printer = new CSVPrinter(outputStreamWriter, CSVFormat.DEFAULT.withHeader(cloumns));
        try {
            statement = connection.createStatement();
             resultSet = statement.executeQuery("select * from vbapff3dddbd28479f2c3e0c37f7_parquet limit 10");
            printer.printRecords(resultSet);
            printer.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
