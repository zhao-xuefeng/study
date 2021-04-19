package TrainFlinkStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import flinksql.Person;


import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;


import java.util.HashMap;
import java.util.Map;

public class FlinkSqlDemoJ {
    public static void main(String[] args) {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(env);

        DataSet<String> dataSet=env.readTextFile("D:\\privateLearn\\data\\person.txt");

//        DataSet<Tuple2<String,String>> dataSet1=dataSet.map(new MapFunction<String, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> map(String s) throws Exception {
//                String []arrs=s.split(" ");
//                return new Tuple2<>(arrs[0],arrs[1]);
//            }
//        });

        DataSet<Person> dataSet1=dataSet.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String s) throws Exception {
                String []line=s.split(" ");
                Person person=new Person();
                person.setName(line[0]);
                person.setAge(line[1]);
                return person;
            }
        });
//        Table table=fbTableEnv.fromDataSet(dataSet1,"name,age");
//        Table table1=table.select("name");
        Table table=fbTableEnv.fromDataSet(dataSet1);
         fbTableEnv.createTemporaryView("person",dataSet1);
         fbTableEnv.registerTable("person",table);
        Table table1=fbTableEnv.scan("person");
         Table table2=table1.select("name");
        Table table3=fbTableEnv.sqlQuery("select name,age from person");

        DataSet<Person> stringDataSet=fbTableEnv.toDataSet(table3,Person.class);
        try {
            stringDataSet.map(new MapFunction<Person, Object>() {
                @Override
                public Object map(Person person) throws Exception {
                    return person.getName()+"-------"+person.getAge();
                }
            }).print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//
///**
// * @author: lipei
// * @Date:2020-03-10 22:03
// */
//public class BatchTableDemo {
//    public static void main(String[] args) {
//        //获取运行环境
//        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
//
//        //创建一个tableEnvironment
//        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.getTableEnvironment(fbEnv);
//
//
//        //读取数据源
//        DataSet<String> ds1 = fbEnv.readTextFile("src/file/text01.txt");
//
//        //数据转换
//        DataSet<Tuple2<String, String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, String>>() {
//            private static final long serialVersionUID = -3027796541526131219L;
//
//            @Override
//            public Tuple2<String, String> map(String s) throws Exception {
//                String[] splits =  s.split(",");
//                return new Tuple2<>(splits[0], splits[1]);
//            }
//        });
//
//        //DataSet 转table, 指定字段名
//        Table table = fbTableEnv.fromDataSet(ds2, "id,name");
//
//
//        Table table02 = table.select("name");
//
//        //将表转换DataSet
//        DataSet<String> ds3  = fbTableEnv.toDataSet(table02, String.class);
//
//        try {
//            ds3.print();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//}

