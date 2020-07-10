package org.github.datapipeline.core.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.config.NodeData;

import java.util.Map;

public class BatchWriterFactory {

    public static final String WRITE = "write";

    private static final String FORMAT = "format";

    public static void createWriter(SparkSession sparkSession, Dataset<Row> ancestorDataset, NodeData graphNode) {
        DataFrameWriter<Row> dataFrameWriter = ancestorDataset.write();
        for (Map.Entry<String, Object> entry : graphNode.getConfig().getAll().entrySet()) {
            String key = entry.getKey();
            String value = (String) entry.getValue();
            if (StringUtils.equals(key, FORMAT)) {
                dataFrameWriter.format(value);
            } else {
                dataFrameWriter.option(key, value);
            }
        }
        dataFrameWriter.save();
    }
}
