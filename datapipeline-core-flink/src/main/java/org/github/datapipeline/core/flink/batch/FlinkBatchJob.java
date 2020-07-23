package org.github.datapipeline.core.flink.batch;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.github.datapipeline.core.flink.config.JobConfig;
import org.github.datapipeline.core.flink.config.NodeData;
import org.github.datapipeline.core.flink.factory.BatchReaderFactory;
import org.github.datapipeline.core.flink.factory.BatchTableFactory;
import org.github.datapipeline.core.flink.factory.BatchWriterFactory;

public class FlinkBatchJob {

    public static final String BATCH = "batch";

    public static void run(ExecutionEnvironment env, JobConfig jobConfig) {
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        for (NodeData graphNode : jobConfig.getGraphNodes()) {
            if (StringUtils.equals(graphNode.getType(), BatchReaderFactory.READ)) {
                BatchReaderFactory.createReader(env, tableEnv, graphNode);
            } else if (StringUtils.equals(graphNode.getType(), BatchWriterFactory.WRITE)) {
                BatchWriterFactory.createWriter(env, tableEnv, graphNode);
            } else {
                BatchTableFactory.evaluateTable(env, tableEnv, graphNode);
            }
        }
    }

}
