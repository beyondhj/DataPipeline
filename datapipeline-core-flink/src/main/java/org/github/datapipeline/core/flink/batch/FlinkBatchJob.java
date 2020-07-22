package org.github.datapipeline.core.flink.batch;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.github.datapipeline.core.flink.config.JobConfig;
import org.github.datapipeline.core.flink.config.NodeData;
import org.github.datapipeline.core.flink.factory.BatchReaderFactory;
import org.github.datapipeline.core.flink.factory.BatchTableFactory;
import org.github.datapipeline.core.flink.factory.BatchWriterFactory;

import java.util.Map;

public class FlinkBatchJob {

    public static final String BATCH = "batch";

    public static void run(ExecutionEnvironment env, JobConfig jobConfig) {
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        Map<String, Table> tables = Maps.newHashMap();
        for (NodeData graphNode : jobConfig.getGraphNodes()) {
            Table ancestorTable = getAncestorTable(jobConfig, tables, graphNode);
            if (StringUtils.equals(graphNode.getType(), BatchReaderFactory.READ)) {
                Table table = BatchReaderFactory.createReader(env, tableEnv, graphNode);
                tables.put(graphNode.getId(), table);
            } else if (StringUtils.equals(graphNode.getType(), BatchWriterFactory.WRITE)) {
                BatchWriterFactory.createWriter(env, tableEnv, ancestorTable, graphNode);
            } else {
                Table table = BatchTableFactory.createTable(env, ancestorTable, graphNode);
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
