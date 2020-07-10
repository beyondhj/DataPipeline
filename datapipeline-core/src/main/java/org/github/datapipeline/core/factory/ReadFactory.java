package org.github.datapipeline.core.factory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.config.NodeData;

public class ReadFactory {

    public static final String READ = "read";

    public static Dataset<Row> createRead(SparkSession sparkSession, NodeData graphNode) {
        return null;
    }
}
