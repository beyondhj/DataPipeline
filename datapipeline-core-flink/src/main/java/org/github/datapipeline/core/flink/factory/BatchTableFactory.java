package org.github.datapipeline.core.flink.factory;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.github.datapipeline.core.flink.config.NodeData;

public class BatchTableFactory {

    public static Table evaluateTable(ExecutionEnvironment evn, BatchTableEnvironment tableEnv, Table ancestorTable, NodeData graphNode) {
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
