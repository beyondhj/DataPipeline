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

    public static Dataset<Row> createReader(SparkSession sparkSession, NodeData graphNode) {
        DataStreamReader dataFrameReader = sparkSession.readStream();
        for (Map.Entry<String, Object> entry : graphNode.getConfig().getAll().entrySet()) {
            String key = entry.getKey();
            String value = (String) entry.getValue();
            if (StringUtils.equals(key, FORMAT)) {
                dataFrameReader.format(value);
            } else {
                dataFrameReader.option(key, value);
            }
        }
        return dataFrameReader.load();
    }
}
