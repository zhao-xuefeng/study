package readfile;

import java.io.*;

public class ReadLineFile {
    /*
    * 按行读文件，然后写到另一个文件中去
    *
    *
    * */
    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("D:\\wps\\要删除的数据集（最终版）.txt"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("D:\\wps\\tablename3"));

        String line = null;
        while ((line = bufferedReader.readLine()) != null) { //按行读
            String[] path=line.split("/");
            String table=path[path.length-1]+"_parquet";
            bufferedWriter.write(table);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }
        bufferedReader.close();
        bufferedWriter.close();

    }
}
