package flink.api.train.streaming;


import flinksql.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamApiTrain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stn = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Person> dataSet = stn.fromElements(new Person("tom", "12"),
//                new Person("tom2", "13"),
//                new Person("tom4", "14"),
//                new Person("tom3", "23"),
//                new Person("tom1", "18"),
//                new Person("tom6", "15"),
//                new Person("tom9", "11"),
//                new Person("tom45", "8"),
//                new Person("tom32", "19"));

        DataStreamSource<String> dataSet=stn.socketTextStream("localhost",9999);

         dataSet.filter(new FilterFunction<String>() {
             @Override
             public boolean filter(String s) throws Exception {
                 return Integer.valueOf(s)>20;
             }


//             @Override
//            public boolean filter(Person person) throws Exception {
//                return Integer.valueOf(person.getAge()) > 12;
//            }
        }).print();
        stn.execute();

    }
}
