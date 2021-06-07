package flink.business_scenarios;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class OrderSource extends RichSourceFunction<Order> {
    private Boolean isRunning=true;
    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (isRunning){
            Random random = new Random();
            while (isRunning) {
                Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                TimeUnit.SECONDS.sleep(1);
                ctx.collect(order);
            }
        }
    }
    @Override
    public void cancel() {
        isRunning=false;
    }
}
