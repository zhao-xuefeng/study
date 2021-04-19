package flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;


import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkHiveDemo {
    public static void main(String[] args) {
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        EnvironmentSettings settings=EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, settings);
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "hive";
        String defaultDatabase = "myflink";
        String hiveConfDir     = "flink/src/main/resources/";

        Catalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//        System.out.println(hive.getHiveVersion());
        tableEnv.registerCatalog("hive",hive);

        tableEnv.useCatalog("hive");
        tableEnv.executeSql("create table hero_info(name string,age string)").print();
        tableEnv.executeSql("show tables").print();

//        tableEnv.executeSql("select * from hero_info").print();

    }
}
