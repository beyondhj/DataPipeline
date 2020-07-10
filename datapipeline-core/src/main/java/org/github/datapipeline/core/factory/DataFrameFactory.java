package org.github.datapipeline.core.factory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.config.NodeData;

public class DataFrameFactory {

    public static Dataset<Row> createDataFrame(SparkSession sparkSession, Dataset<Row> ancestorDataset, NodeData graphNode) {
        return null;
    }
}
