package org.github.datapipeline.core.batch;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.config.JobConfig;
import org.github.datapipeline.core.config.NodeData;
import org.github.datapipeline.core.factory.DataFrameFactory;
import org.github.datapipeline.core.factory.ReaderFactory;
import org.github.datapipeline.core.factory.WriterFactory;

import java.util.Map;

public class SparkBatchJob {

    public static final String BATCH = "batch";

    public static void run(SparkSession sparkSession, JobConfig jobConfig) {
        Map<String, Dataset<Row>> nodeDataset = Maps.newHashMap();
        for (NodeData graphNode : jobConfig.getGraphNodes()) {
            Dataset<Row> ancestorDataset = getAncestorDataset(jobConfig, nodeDataset, graphNode);
            if (StringUtils.equals(graphNode.getType(), ReaderFactory.READ)) {
                Dataset<Row> dataFrame = ReaderFactory.createReader(sparkSession, graphNode);
                nodeDataset.put(graphNode.getId(), dataFrame);
            } else if (StringUtils.equals(graphNode.getType(), WriterFactory.WRITE)) {
                WriterFactory.createWriter(sparkSession, ancestorDataset, graphNode);
            } else {
                Dataset<Row> dataFrame = DataFrameFactory.createDataFrame(sparkSession, ancestorDataset, graphNode);
                nodeDataset.put(graphNode.getId(), dataFrame);
            }
        }
    }

    private static Dataset<Row> getAncestorDataset(JobConfig jobConfig, Map<String, Dataset<Row>> nodeDataset, NodeData graphNode) {
        Dataset<Row> ancestorDataset = null;
        NodeData ancestorNode = jobConfig.getGraphAncestor(graphNode);
        if (ancestorNode != null) {
            ancestorDataset = nodeDataset.get(ancestorNode.getId());
        }
        return ancestorDataset;
    }
}
