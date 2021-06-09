import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class TestSparkLuncher {
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

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        HashMap env = new HashMap();
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR", "D:\\bonc\\soft\\hadoop\\hadoop-2.6.1\\etc\\hadoop");
        env.put("JAVA_HOME", "D:\\bonc\\jdk\\");
        env.put("SPARK_HOME","D:\\bonc\\soft\\spark\\spark\\spark");
        env.put("HADOOP_HOME","D:\\bonc\\soft\\hadoop\\hadoop-2.6.1");
        SparkLauncher sparkLauncher = new SparkLauncher(env);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        YarnClient yarn=getYarnClient();
        SparkAppHandle sparkAppHandle = sparkLauncher
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setMainClass("org.apache.spark.examples.SparkPi")
                .setAppResource("D:\\bonc\\soft\\spark\\spark\\spark\\examples\\spark-examples_2.11-2.4.1.jar")
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {

                        if (sparkAppHandle.getState().isFinal()) {
                            countDownLatch.countDown();
                            System.out.println(sparkAppHandle.getAppId()+"--------");
                            System.out.println(sparkAppHandle.getState()+"{{{{{");
                            System.out.println( sparkAppHandle.getState());
                            ApplicationId appId = ConverterUtils.toApplicationId(sparkAppHandle.getAppId());
                            try {
                               yarn.killApplication(appId);
                               yarn.close();
                            } catch (YarnException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }


                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {

                    }
                });

        countDownLatch.await();
        System.out.println("jieshu");

    }

}
