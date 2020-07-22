package org.github.datapipeline.core.flink.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.github.datapipeline.core.flink.config.NodeData;

public class BatchWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static void createWriter(ExecutionEnvironment evn, BatchTableEnvironment tableEnv, Table ancestorTable, NodeData graphNode) {
        DataSet<Row> dataSet = tableEnv.toDataSet(ancestorTable, Row.class);
        dataSet.output(createOutputFormat(graphNode));
    }

    private static boolean invalidValue(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return true;
        }
        return false;
    }

    private static OutputFormat createOutputFormat(NodeData graphNode) {
        //TODO
        return null;
    }
}
