package flink.business_scenarios.orderdemo;

import flink.api.train.streaming.RichFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order{
    private String orderId;
    private Integer userId;
    private Integer money;
    private Long createTime;//事件时间

}
