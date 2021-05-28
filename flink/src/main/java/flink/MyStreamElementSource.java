package flink;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyStreamElementSource implements SourceFunction<Tuple3<Integer, Integer, Integer>> {

    boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Integer>> ctx) throws Exception {
        int a = 0, b = 0, c = 0;
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        while (isRunning) {
            if (a>=0) {
                a = list.get(new Random().nextInt(7));
                b = new Random().nextInt(7);
                c = new Random().nextInt(7);
            } else {
                isRunning = false;
            }
            ctx.collect(new Tuple3<Integer, Integer, Integer>(a, b, c));
            Thread.sleep(3000l);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
