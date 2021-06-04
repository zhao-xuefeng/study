package flink.tablesql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableSqlWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment  stenv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings env= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment table=StreamTableEnvironment.create(stenv,env);


    }
}
