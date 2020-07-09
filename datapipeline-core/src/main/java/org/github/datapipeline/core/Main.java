package org.github.datapipeline.core;

import org.github.datapipeline.core.config.JobConfig;
import org.github.datapipeline.core.config.NodeData;
import org.github.datapipeline.core.options.JobOptions;
import org.github.datapipeline.core.options.JobOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        JobOptions jobOptions = JobOptionsParser.parse(args).getJobOptions();
        JobConfig jobConfig = JobConfig.parse(jobOptions.getJobJson());

        for (NodeData graphNode : jobConfig.getGraphNodes()) {

        }
    }
}
