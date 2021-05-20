package flink.api.train.streaming;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.hadoop2.org.codehaus.jackson.map.TypeSerializer;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class RichFunction  extends RichSourceFunction {
    private volatile boolean running = true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        int count=1;
        while (running){
            if (count>200){
                running=false;
            }
            count++;
        }
        ctx.collect(count);
    }

    @Override
    public void cancel() {
        running=false;
    }

    public class RichMap extends RichMapFunction<String,String>{
        private MapState mapState=null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String,String> mapStateDescriptor=new MapStateDescriptor<String, String>("person",
                    String.class,
                    String.class );
            mapState= getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public String map(String value) throws Exception {
            return null;
        }
    }
}
