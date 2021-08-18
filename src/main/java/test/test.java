package test;

import java.io.*;

public class test {
    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("D:\\wps\\aa.csv"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("D:\\wps\\tablename2"));

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
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
