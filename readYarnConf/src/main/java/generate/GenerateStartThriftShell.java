package generate;


import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class GenerateStartThriftShell {
    private CommandLine commandLine = null;
    private String queue = "";
    private String scheduler = "";
    private String kryo_max = "";
    private GenerateFile generateFile = new GenerateFile();


    public void parseCommand(String[] params) throws ParseException, IOException, YarnException {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption("help", "print this message");
        options.addOption("queue", "queue", true, "The name of the queue used");
        options.addOption("scheduler", "schedulerMode", true, "spark.scheduler.mode  ");
//        options.addOption("fsCache", "cache", true, "spark.hadoop.fs.hdfs.impl.disable.cache,the value is either true or false");
        options.addOption("kryo_max", "kryoserializer.buffer.max", true, "spark.kryoserializer.buffer.max");
//        options.addOption("partition", "sql.sources.partitionColumnTypeInference", true, "spark.sql.sources.partitionColumnTypeInference.enabled,the value is either true or false");
//        options.addOption("shuffle","shuffle.service.enabled",true,"shuffle.service.enabled,the value is either true or false");
//        options.addOption("autoRepartition","spark.sql.auto.repartition",true,"spark.sql.auto.repartition,the value is either true or false") ;
//        options.addOption("adaptive","spark.sql.adaptive.enabled",true,"spark.sql.adaptive.enabled,the value is either true or false");
//        options.addOption("scheduler_file", "spark.scheduler.allocation.file", true, "spark.scheduler.allocation.file,the value is file path");
//        options.addOption("incrementalCollect","",true,"spark.sql.thriftServer.incrementalCollect,the value is either true or false");
        commandLine = parser.parse(options, params);



        if (commandLine.hasOption("queue")) {
            queue = commandLine.getOptionValue("queue");
        }

        if (commandLine.hasOption("scheduler")) {
            scheduler = commandLine.getOptionValue("scheduler");
        }

        if (commandLine.hasOption("kryo_max")) {
            kryo_max = commandLine.getOptionValue("kryo_max");
        }
//        if (commandLine.hasOption("scheduler_file")) {
//            scheduler_file = commandLine.getOptionValue("scheduler_file");
//        }

        if (commandLine.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
        }else {
            generateFile.generateXmlFile(scheduler);
            generateFile.generateThriftserverShellFile(queue, scheduler, kryo_max);
        }
    }

    public static void main(String[] args) throws ParseException, YarnException, IOException {
        GenerateStartThriftShell generate = new GenerateStartThriftShell();
//        String[] params = {"-queue", "default", "-scheduler", "FIFO", "-kryo_max", "1024"};
        String[] params={"-help"};
        generate.parseCommand(params);
    }

}


