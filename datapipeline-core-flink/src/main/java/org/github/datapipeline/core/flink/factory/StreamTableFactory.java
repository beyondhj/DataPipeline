package org.github.datapipeline.core.flink.factory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.github.datapipeline.core.flink.config.NodeData;

public class StreamTableFactory {

    public static Table evaluateTable(StreamExecutionEnvironment evn, StreamTableEnvironment tableEnv, NodeData graphNode) {
        Table table = tableEnv.sqlQuery(getQuery(graphNode));
        tableEnv.createTemporaryView(getTableName(graphNode), table);
        return table;
    }

    private static String getQuery(NodeData graphNode) {
        //TODO
        return null;
    }

    private static String getTableName(NodeData graphNode) {
        //TODO
        return null;
    }
}
