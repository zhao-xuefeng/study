package datahub.dataconsumer;


import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatahubConsumer1 {
    //点位消费示例，并在消费过程中进行点位的提交
    public void offset_consumption(int maxRetry) {
        String endpoint = "https://dh-cn-beijing.aliyuncs.com";
        String accessId = "";
        String accessKey = "";  //你的账号信息
        String projectName = "demo_1";
        String topicName = "topic_1";
        String subId = "1606724719251WOGIU";
        String shardId = "0";
        List<String> shardIds = Arrays.asList(shardId);
        // 创建DataHubClient实例
        DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                .build();
        RecordSchema schema = datahubClient.getTopic(projectName, topicName).getRecordSchema();
        OpenSubscriptionSessionResult openSubscriptionSessionResult = datahubClient.openSubscriptionSession(projectName, topicName, subId, shardIds);
        SubscriptionOffset subscriptionOffset = openSubscriptionSessionResult.getOffsets().get(shardId);
        // 1、获取当前点位的cursor，如果当前点位已过期则获取生命周期内第一条record的cursor，未消费同样获取生命周期内第一条record的cursor
        String cursor = "";
        //sequence < 0说明未消费
        if (subscriptionOffset.getSequence() < 0) {
            // 获取生命周期内第一条record的cursor
            cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
        } else {
            // 获取下一条记录的Cursor
            long nextSequence = subscriptionOffset.getSequence() + 1;
            try {
                //按照SEQUENCE getCursor可能报SeekOutOfRange错误，表示当前cursor的数据已过期
                cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (SeekOutOfRangeException e) {
                // 获取生命周期内第一条record的cursor
                cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
            }
        }
        // 2、读取并保存点位，这里以读取Tuple数据为例，并且每1000条记录保存一次点位
        long recordCount = 0L;
        // 每次读取1000条record
        int fetchNum = 5;
        int retryNum = 0;
        int commitNum = 5;
        while (retryNum < maxRetry) {
            try {
                GetRecordsResult getRecordsResult = datahubClient.getRecords(projectName, topicName, shardId, schema, cursor, fetchNum);
                if (getRecordsResult.getRecordCount() <= 0) {
                    // 无数据，sleep后读取
                    System.out.println("no data, sleep 1 second");
                    Thread.sleep(1000);
                    continue;
                }
                for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
                    //消费数据
                    TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();
                    System.out.println("name:" + data.getField("name") + "\t"
                            + "age:" + data.getField("age") + "\t" + "gender:" + data.getField("gender") + "\t" + "adderss:" + data.getField("address"));
                    // 处理数据完成后，设置点位
                    recordCount++;
                    subscriptionOffset.setSequence(recordEntry.getSequence());
                    subscriptionOffset.setTimestamp(recordEntry.getSystemTime());
                    // commit offset every 1000 records
                    if (recordCount % commitNum == 0) {
                        //提交点位点位
                        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
                        offsetMap.put(shardId, subscriptionOffset);
                        datahubClient.commitSubscriptionOffset(projectName, topicName, subId, offsetMap);
                        System.out.println("commit offset successful");
                    }
                }
                cursor = getRecordsResult.getNextCursor();

            } catch (SubscriptionOfflineException | SubscriptionSessionInvalidException e) {
                // 退出. Offline: 订阅下线; SessionChange: 表示订阅被其他客户端同时消费
                e.printStackTrace();
                throw e;
            } catch (SubscriptionOffsetResetException e) {
                // 点位被重置，需要重新获取SubscriptionOffset版本信息
                SubscriptionOffset offset = datahubClient.getSubscriptionOffset(projectName, topicName, subId, shardIds).getOffsets().get(shardId);
                subscriptionOffset.setVersionId(offset.getVersionId());
                // 点位被重置之后，需要重新获取点位，获取点位的方法应该与重置点位时一致，
                // 如果重置点位时，同时设置了sequence和timestamp，那么既可以用SEQUENCE获取，也可以用SYSTEM_TIME获取
                // 如果重置点位时，只设置了sequence，那么只能用sequence获取，
                // 如果重置点位时，只设置了timestamp，那么只能用SYSTEM_TIME获取点位
                // 一般情况下，优先使用SEQUENCE，其次是SYSTEM_TIME,如果都失败，则采用OLDEST获取
                cursor = null;
                if (cursor == null) {
                    try {
                        long nextSequence = offset.getSequence() + 1;
                        cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
                        System.out.println("get cursor successful");
                    } catch (DatahubClientException exception) {
                        System.out.println("get cursor by SEQUENCE failed, try to get cursor by SYSTEM_TIME");
                    }
                }
                if (cursor == null) {
                    try {
                        cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.SYSTEM_TIME, offset.getTimestamp()).getCursor();
                        System.out.println("get cursor successful");
                    } catch (DatahubClientException exception) {
                        System.out.println("get cursor by SYSTEM_TIME failed, try to get cursor by OLDEST");
                    }
                }
                if (cursor == null) {
                    try {
                        cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
                        System.out.println("get cursor successful");
                    } catch (DatahubClientException exception) {
                        System.out.println("get cursor by OLDEST failed");
                        System.out.println("get cursor failed!!");
                        throw e;
                    }
                }
            } catch (LimitExceededException e) {
                // limit exceed, retry
                e.printStackTrace();
                retryNum++;
            } catch (DatahubClientException e) {
                // other error, retry
                e.printStackTrace();
                retryNum++;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    public static void main(String[] args) {
        new DatahubConsumer1().offset_consumption(1);
    }
}

