package generate;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateFile {
    private File file=new File("fairscheduler.xml");
    private static String thriftShell = "#!/bin/bash \n" +
            "thriftserver=`which start-thriftserver.sh` \n" +
            "$thriftserver \\\n" +
            "        --master yarn \\\n" +
            "        --deploy-mode client \\\n" +
            "        --queue ${queue} \\\n" +
            "        --num-executors ${numExecutors} \\\n" +
            "        --hiveconf hive.server2.thrift.port 10001 \\\n" +
            "        --hiveconf hive.server2.thrift.min.worker.threads 20 \\\n" +
            "        --hiveconf hive.server2.thrift.max.worker.threads 1000 \\\n" +
            "        --conf spark.driver.memory=${driverMemory}G \\\n" +
            "        --conf spark.executor.memory=${executorMemory}G \\\n" +
            "        --conf spark.executor.cores=${executorCores} \\\n" +
            "        --conf spark.scheduler.mode=${scheduler} \\\n" +
            "        --conf spark.driver.maxResultSize=${maxResultSize}G \\\n" +
            "        --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \\\n" +
            "        --conf spark.kryoserializer.buffer.max=${kryo_max}m \\\n" +
            "        --conf spark.sql.sources.partitionColumnTypeInference.enabled=false \\\n" +
            "        --conf spark.shuffle.service.enabled=true \\\n" +
            "        --conf spark.sql.auto.repartition=true \\\n" +
            "        --conf spark.sql.adaptive.enabled=true \\\n" +
            "        --conf spark.scheduler.allocation.file=${scheduler_file} \\\n" +
            "        --conf spark.sql.thriftServer.incrementalCollect=true\n";

    public void generateXmlFile(String schedulerType) {
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("allocations");
        int[] ratio = {6, 1, 1, 2};
        List<String> list = new ArrayList<>();
        list.add("default");
        list.add("low");
        list.add("mid");
        list.add("high");
        int ratioLocation = 0;
        int defaultWeight = 10;
        int defaulMinCores = 5;
        for (String s : list) {
            Element pool = root.addElement("pool");
            pool.addAttribute("name", s);
            Element schedulingMode = pool.addElement("schedulingMode");
            schedulingMode.addText(schedulerType);
            Element weights = pool.addElement("weight");
            Element minShare = pool.addElement("minShare");
            weights.addText(String.valueOf(ratio[ratioLocation] * defaultWeight));
            minShare.addText(String.valueOf(ratio[ratioLocation] * defaulMinCores));
            ratioLocation++;
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            XMLWriter xmlWriter = new XMLWriter(fos);
            xmlWriter.write(doc);
            xmlWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateThriftserverShellFile(String queue, String scheduler, String kryoMax) throws IOException, YarnException {
        Map<String, Object> options = new HashMap<>();
        options.put("queue", queue);
        options.put("scheduler", scheduler);
        options.put("kryo_max", kryoMax);
//        options.put("scheduler_file",schedulerFile);

        Integer totalNodes = GetClusterConf.getClusterTotalMemoryAndCoes().get("totalNodes");
        Integer totalMemory = GetClusterConf.getClusterTotalMemoryAndCoes().get("totalMemory");
//        Integer totalMemory=52;
//        Integer totalNodes=4;
        Integer executorMemory = totalMemory / 3;
        Integer driverMemory = totalNodes * totalMemory / 10;
        Integer executorCores = executorMemory / 2;
        Integer maxResultSize = driverMemory / 2;
        options.put("numExecutors", totalNodes);
        options.put("driverMemory", driverMemory);
        options.put("executorMemory", executorMemory);
        options.put("executorCores", executorCores);
        options.put("maxResultSize", maxResultSize);
        options.put("scheduler_file", file.getAbsolutePath());

        StringSubstitutor sub = new StringSubstitutor(options);
        String shellContent = sub.replace(thriftShell);
        try (PrintWriter fileWriter = new PrintWriter("start-thriftserver.sh")) {
            fileWriter.println(shellContent);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

