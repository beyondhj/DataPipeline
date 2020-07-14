package org.github.datapipeline.core.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.github.datapipeline.core.config.NodeData;

import java.util.Map;

public class StreamReaderFactory {

    public static final String READ = "read";

    private static final String FORMAT = "format";

    private static final String SCHEMA = "schema";

    public static Dataset<Row> createReader(SparkSession sparkSession, NodeData graphNode) {
        DataStreamReader dataFrameReader = sparkSession.readStream();
        for (Map.Entry<String, Object> entry : graphNode.getConfig().getAll().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (invalidValue(value)) {
                continue;
            }
            if (StringUtils.equals(key, FORMAT)) {
                dataFrameReader.format((String) value);
            } else if (StringUtils.equals(key, SCHEMA)) {
                dataFrameReader.schema((String) value);
            } else if (value instanceof String) {
                dataFrameReader.option(key, (String) value);
            } else if (value instanceof Long) {
                dataFrameReader.option(key, (Long) value);
            } else if (value instanceof Integer) {
                dataFrameReader.option(key, (Integer) value);
            } else if (value instanceof Double) {
                dataFrameReader.option(key, (Double) value);
            } else if (value instanceof Boolean) {
                dataFrameReader.option(key, (Boolean) value);
            }
        }
        return dataFrameReader.load();
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
}
