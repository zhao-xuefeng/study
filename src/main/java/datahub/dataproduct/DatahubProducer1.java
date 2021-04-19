package datahub.dataproduct;


import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.AuthorizationFailureException;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public class DatahubProducer1 {
    String endpoint = "https://dh-cn-beijing.aliyuncs.com";
    String accessId = "";
    String accessKey = ""; //你的账号信息
    String projectName = "demo_1";
    String topicName = "topic_1";

    //    String subId = "1606724719251WOGIU";
//    String shardId = "0";
//    private DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
//            .setDatahubConfig(
//                    new DatahubConfig(endpoint,
//                            // 是否开启二进制传输，服务端2.12版本开始支持
//                            new AliyunAccount(accessId, accessKey), true))
//            .build();
    // 写入Tuple型数据
    public void tupleExample() throws ResourceNotFoundException {
        DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                .build();
        String shardId = "0";
        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(projectName, topicName).getRecordSchema();
        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            RecordEntry recordEntry = new RecordEntry();
            // 对每条数据设置额外属性，例如ip 机器名等。可以不设置额外属性，不影响数据写入
            recordEntry.addAttribute("key1", "value1");
            TupleRecordData data = new TupleRecordData(recordSchema);
            data.setField("name", "美少女" + i);
            data.setField("age", "1" + i);
            data.setField("gender", "女");
            data.setField("address", "北京" + i);
            data.setField("memo", "这仅仅是个美少女呀");
            recordEntry.setRecordData(data);
            recordEntry.setShardId(shardId);
            recordEntries.add(recordEntry);
        }
        try {
            // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
            //datahubClient.putRecordsByShard(Constant.projectName, Constant.topicName, shardId, recordEntries);
            datahubClient.putRecords(projectName, topicName, recordEntries);
            System.out.println("write data successful");
        } catch (InvalidParameterException e) {
            System.out.println("invalid parameter, please check your parameter");
            System.exit(1);
        } catch (AuthorizationFailureException e) {
            System.out.println("AK error, please check your accessId and accessKey");
            System.exit(1);
        } catch (ShardSealedException e) {
            System.out.println("shard status is CLOSED, can not write");
            System.exit(1);
        } catch (DatahubClientException e) {
            System.out.println("other error");
            System.out.println(e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        DatahubProducer1 datahubProducer1 = new DatahubProducer1();
        datahubProducer1.tupleExample();
    }
}
