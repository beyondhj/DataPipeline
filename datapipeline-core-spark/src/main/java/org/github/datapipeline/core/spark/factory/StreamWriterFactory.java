package org.github.datapipeline.core.spark.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.github.datapipeline.core.spark.config.NodeData;

import java.util.Map;

public class StreamWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static void createWriter(SparkSession sparkSession, Dataset<Row> ancestorDataset, NodeData graphNode) {
        DataStreamWriter<Row> dataFrameWriter = ancestorDataset.writeStream();
        for (Map.Entry<String, Object> entry : graphNode.getConfig().getAll().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (invalidValue(value)) {
                continue;
            }
            if (StringUtils.equals(key, FORMAT)) {
                dataFrameWriter.format((String) value);
            } else if (value instanceof String) {
                dataFrameWriter.option(key, (String) value);
            } else if (value instanceof Long) {
                dataFrameWriter.option(key, (Long) value);
            } else if (value instanceof Integer) {
                dataFrameWriter.option(key, (Integer) value);
            } else if (value instanceof Double) {
                dataFrameWriter.option(key, (Double) value);
            } else if (value instanceof Boolean) {
                dataFrameWriter.option(key, (Boolean) value);
            }
        }
        dataFrameWriter.start();
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
