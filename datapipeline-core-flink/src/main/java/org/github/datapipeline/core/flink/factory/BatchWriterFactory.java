package org.github.datapipeline.core.flink.factory;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.github.datapipeline.core.flink.config.NodeData;

public class BatchWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static TableResult createWriter(ExecutionEnvironment evn, BatchTableEnvironment tableEnv, Table ancestorTable, NodeData graphNode) {
        tableEnv.executeSql(getOutputDDL(graphNode));
        return ancestorTable.executeInsert(getOutputTable(graphNode));
    }

    private static String getOutputDDL(NodeData graphNode) {
        //TODO
        return null;
    }

    private static String getOutputTable(NodeData graphNode) {
        //TODO
        return null;
    }

}
