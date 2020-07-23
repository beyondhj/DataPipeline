package org.github.datapipeline.core.flink.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.github.datapipeline.core.flink.config.NodeData;

import java.util.Collections;
import java.util.List;

public class BatchTableFactory {

    public static Table evaluateTable(ExecutionEnvironment evn, BatchTableEnvironment tableEnv, NodeData graphNode) {
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
