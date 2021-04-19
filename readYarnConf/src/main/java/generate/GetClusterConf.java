package generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetClusterConf {
    private final static String CORE_SITE = "core-site.xml";
    private final static String YARN_SITE = "yarn-site.xml";
    private final static String HDFS_SITE = "hdfs-site.xml";
    private final static String MAPRED_SITE = "mapred-site.xml";
    private static YarnClient yarnClient = null;
    private static int totalMemory = 0;
    private static int totalVirtualCores = 0;

    public static Configuration loadClusterConf() {
        Configuration conf = new Configuration();
        conf.addResource(CORE_SITE);
        conf.addResource(YARN_SITE);
        conf.addResource(HDFS_SITE);
        conf.addResource(MAPRED_SITE);
        return conf;
    }

    public static YarnClient getYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(loadClusterConf());
        yarnClient.start();
        return yarnClient;
    }

    public static void closeYarnClient() throws IOException {
        if (yarnClient != null) {
            yarnClient.close();
        }
    }

    //    memory单位为G
    public static Map<String,Integer> getClusterTotalMemoryAndCoes() throws YarnException, IOException {
        List<NodeReport> nodeReportList=getClusterInfo();
        Map<String,Integer> memoryAndCores=new HashMap<>();
        if (!nodeReportList.isEmpty()) {
            Resource resource = nodeReportList.get(0).getCapability();
            totalMemory=(int)resource.getMemory()/1024;    //在高版本中 被舍弃
//            totalMemory = (int) resource.getMemorySize() / 1024; 此方法在hadoop版本更高的才支持
            totalVirtualCores = resource.getVirtualCores();
        } else {
            throw new YarnException("Failed to get node information");
        }
        memoryAndCores.put("totalMemory",totalMemory);
        memoryAndCores.put("totalVirtualCores",totalVirtualCores);
        memoryAndCores.put("totalNodes",nodeReportList.size());
        closeYarnClient();
        return memoryAndCores;
    }
    public static List<NodeReport> getClusterInfo() throws IOException, YarnException {
        yarnClient = getYarnClient();
        List<NodeReport> nodeReportList = yarnClient.getNodeReports();
        return nodeReportList;
    }


}

