package org.github.datapipeline.core.flink.factory;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.github.datapipeline.core.flink.config.NodeData;

public class BatchReaderFactory {

    public static final String READ = "read";

    private static final String FORMAT = "format";

    private static final String SCHEMA = "schema";

    public static TableResult createReader(ExecutionEnvironment env, BatchTableEnvironment tableEnv, NodeData graphNode) {
        return tableEnv.executeSql(getInputDDL(graphNode));
    }


    private static String getInputDDL(NodeData graphNode) {
        //TODO
        return null;
    }


}
