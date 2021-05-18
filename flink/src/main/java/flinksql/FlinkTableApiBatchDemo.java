package flinksql;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import org.apache.flink.types.Row;

public class FlinkTableApiBatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment benv = BatchTableEnvironment.create(env);

        DataSet<StudentsInfo> dataSet =
                env.readCsvFile("D:\\privateLearn\\data\\ss.csv")
                        .ignoreFirstLine().
                        pojoType(StudentsInfo.class, "name", "sex", "cource", "socre");
        Table table = benv.fromDataSet(dataSet);
        benv.registerTable("studentsInfo", table);
        Table counts = benv.scan("studentsInfo").groupBy("name")
                .select("name,cource.count as cnt");
//        Table counts=benv.scan("studentsInfo").select("name,sex");
        DataSet<Row> re = benv.toDataSet(counts, Row.class);
        re.print();


    }
}
