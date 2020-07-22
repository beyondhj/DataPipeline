package org.github.datapipeline.core.flink.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.github.datapipeline.core.flink.config.NodeData;

public class StreamWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static void createWriter(StreamExecutionEnvironment evn, StreamTableEnvironment tableEnv, Table ancestorTable, NodeData graphNode) {
        DataStream<Row> dataStream = tableEnv.toAppendStream(ancestorTable, Row.class);
        dataStream.addSink(createSinkFunction(graphNode));
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

    private static SinkFunction createSinkFunction(NodeData graphNode) {
        //TODO
        return null;
    }
}
