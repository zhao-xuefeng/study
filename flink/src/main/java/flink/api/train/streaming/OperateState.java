package flink.api.train.streaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/*
 * 模拟kafka 维护offset，
 * 真实的FlinkKafkaConsumer  也是这样  只不过liststate 里使用的是map（分区，offeset）
 * */
public class OperateState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(1000l);
        env.setStateBackend(new FsStateBackend("file:///D:\\home\\checkpoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        DataStreamSource<String> da = env.addSource(new Mykafkasource()).setParallelism(1);
        da.print().setParallelism(1);
        env.execute();

    }
    public static class Mykafkasource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        private boolean isRunning = true;
        //-1.声明一个OperatorState来记录offset
        private ListState<Long> offsetState;

        private Long offset = 0L;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //-2.创建状态描述器
            ListStateDescriptor listStateDescriptor = new ListStateDescriptor<Long>("offsetStates", Long.class);
            //-3.根据状态描述器初始化状态
            offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {

                Iterator<Long> iterable = offsetState.get().iterator();
                if (iterable.hasNext()) {
                    offset = iterable.next();
                }
                offset++;
                int subtask = getRuntimeContext().getIndexOfThisSubtask();
                TimeUnit.SECONDS.sleep(2); //睡2秒  做checkpoint
                if (offset%5 == 0) {
                    System.out.println("程序遇到异常了.....");
                    throw new Exception("程序遇到异常了.....");

                }
                ctx.collect("分区:" + subtask + "消费到的offset位置为:" + offset);

            }
        }

        /**
         * 下面的snapshotState方法会按照固定的时间间隔将State信息存储到Checkpoint/磁盘中,也就是在磁盘做快照!
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //-5.保存State到Checkpoint中
            offsetState.clear();//清理内存中存储的offset到Checkpoint中
            //-6.将offset存入State中
            offsetState.add(offset);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
