package flinksql;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class UTC2Local  extends ScalarFunction {
    public Timestamp eval(Long s) {
        return new Timestamp(s); //转换成本地对应时间
    }
    public long eval2(Long s) {
        long timestamp = s + 28800000;  //flink默认的是UTC时间，我们的时区是东八区，时间戳需要增加八个小时
        return timestamp;
    }
}
