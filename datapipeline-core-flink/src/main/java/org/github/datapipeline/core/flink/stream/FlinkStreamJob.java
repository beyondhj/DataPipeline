package org.github.datapipeline.core.flink.stream;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.github.datapipeline.core.flink.config.JobConfig;
import org.github.datapipeline.core.flink.config.NodeData;
import org.github.datapipeline.core.flink.factory.*;

import java.util.Map;

public class FlinkStreamJob {

    public static final String STREAM = "stream";

    public static void run(StreamExecutionEnvironment env, JobConfig jobConfig) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Map<String, Table> tables = Maps.newHashMap();
        for (NodeData graphNode : jobConfig.getGraphNodes()) {
            Table ancestorTable = getAncestorTable(jobConfig, tables, graphNode);
            if (StringUtils.equals(graphNode.getType(), BatchReaderFactory.READ)) {
                StreamReaderFactory.createReader(env, tableEnv, graphNode);
            } else if (StringUtils.equals(graphNode.getType(), BatchWriterFactory.WRITE)) {
                StreamWriterFactory.createWriter(env, tableEnv, ancestorTable, graphNode);
            } else {
                Table table = StreamTableFactory.evaluateTable(env, tableEnv, ancestorTable, graphNode);
                tables.put(graphNode.getId(), table);
            }
        }
    }

    private static Table getAncestorTable(JobConfig jobConfig, Map<String, Table> tables, NodeData graphNode) {
        Table ancestorTable = null;
        NodeData ancestorNode = jobConfig.getGraphAncestor(graphNode);
        if (ancestorNode != null) {
            ancestorTable = tables.get(ancestorNode.getId());
        }
        return ancestorTable;
    }

}
