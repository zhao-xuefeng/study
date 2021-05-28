package flink.api.train.streaming;


import flink.MyStreamElementSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stenv=StreamExecutionEnvironment.getExecutionEnvironment();
/*
* -----:2> (5,6,0)
-----:3> (12,6,6)
-----:4> (12,6,6)
==========:2> (12,12,6)
*
*         source.keyBy(x->x.f0).countWindow(2).sum(1).print("==========");
* 当窗口以元组的第一个分组时，相同的key达到2时 开始计算,sum得位置是计算元组第二个元素的和
*
* */
        DataStreamSource<Tuple3<Integer, Integer, Integer>> source=stenv.addSource(new MyStreamElementSource());
        source.print("-----");
        source.keyBy(x->x.f0).countWindow(2).sum(1).print("==========");

//  带时间戳的滚动窗口，还没有调计算方法

//        source.keyBy(x->x.f0).window(TumblingEventTimeWindows.of(Time.seconds(5l)));
        stenv.execute();
    }

}
