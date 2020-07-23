package org.github.datapipeline.core.flink.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.github.datapipeline.core.flink.config.JobConfig;
import org.github.datapipeline.core.flink.config.NodeData;
import org.github.datapipeline.core.flink.factory.*;

public class FlinkStreamJob {

    public static final String STREAM = "stream";

    public static void run(StreamExecutionEnvironment env, JobConfig jobConfig) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        for (NodeData graphNode : jobConfig.getGraphNodes()) {
            if (StringUtils.equals(graphNode.getType(), BatchReaderFactory.READ)) {
                StreamReaderFactory.createReader(env, tableEnv, graphNode);
            } else if (StringUtils.equals(graphNode.getType(), BatchWriterFactory.WRITE)) {
                StreamWriterFactory.createWriter(env, tableEnv, graphNode);
            } else {
                StreamTableFactory.evaluateTable(env, tableEnv, graphNode);
            }
        }
    }

}
