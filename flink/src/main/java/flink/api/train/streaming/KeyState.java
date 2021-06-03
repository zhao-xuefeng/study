package flink.api.train.streaming;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyResolving;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/*
*
* 自定义求最大值，也可以直接maxby
* */
public class KeyState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRestartStrategy();

        DataStream<Tuple2<String,Integer>> dataStream=env.
                fromElements(new Tuple2<>("北京",1),
                        new Tuple2<>("北京",5),
                        new Tuple2<>("北京",3),
                        new Tuple2<>("上海",1),
                        new Tuple2<>("上海",9),
                        new Tuple2<>("上海",6));
        KeyedStream<Tuple2<String,Integer>,String> keyedStream=dataStream.keyBy(x->x.f0);
        keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple3<String,Integer,Integer>>() {
            private ValueState<Integer> maxData;
            @Override
            public void open(Configuration parameters) throws Exception {
               ValueStateDescriptor<Integer> vs= new ValueStateDescriptor<Integer>("maxdata",Integer.class);
               maxData=getRuntimeContext().getState(vs);
            }
            @Override
            public Tuple3<String, Integer, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Integer stateValue=maxData.value();
                Integer currentValue=value.f1;
                if (stateValue==null || currentValue>stateValue){
                    //记得更新state的值
                    maxData.update(currentValue);
                    return new Tuple3<>(value.f0,currentValue,currentValue);
                }else {
                    return new Tuple3<>(value.f0,currentValue,stateValue);
                }
            }
        }).print();
        env.execute();
    }
}
