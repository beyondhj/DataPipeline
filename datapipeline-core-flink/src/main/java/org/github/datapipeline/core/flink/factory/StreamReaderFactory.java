package org.github.datapipeline.core.flink.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.github.datapipeline.core.flink.config.NodeData;

import java.util.Map;

public class StreamReaderFactory {

    public static final String READ = "read";

    private static final String FORMAT = "format";

    private static final String SCHEMA = "schema";

    public static TableResult createReader(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, NodeData graphNode) {
        return tableEnv.executeSql(getInputDDL(graphNode));
    }


    private static String getInputDDL(NodeData graphNode) {
        //TODO
        return null;
    }

}
