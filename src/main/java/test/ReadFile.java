package test;
import java.io.*;

public class  ReadFile {
    public static void main(String[] args) throws IOException {

                //BufferedReader是可以按行读取文件
                FileInputStream inputStream = new FileInputStream("D:\\wps\\要删除的数据集（最终版）.txt");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                File  file=new File("D:\\table_name3.csv");
                String str = null;
                FileOutputStream fop = new FileOutputStream(file);
                OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
                int count=0;
                while((str = bufferedReader.readLine()) != null)
                {
                    String[]  pathname=str.split("/");
                    String table=pathname[pathname.length-1]+"_parquet"+"\n";
                    writer.write(table);
                    count++;
                    System.out.println(table);
                }
        System.out.println(count);
                //close
//                fop.close();
//                if (writer!=null){
//                    writer.close();
//                }
//
//                inputStream.close();
//                bufferedReader.close();

            }


}
