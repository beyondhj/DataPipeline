package org.github.datapipeline.core.flink.factory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.github.datapipeline.core.flink.config.NodeData;

public class StreamWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static TableResult createWriter(StreamExecutionEnvironment evn, StreamTableEnvironment tableEnv, Table ancestorTable, NodeData graphNode) {
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
