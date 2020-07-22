package org.github.datapipeline.core.spark.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.spark.config.NodeData;

import java.util.Collections;
import java.util.List;

public class DataFrameFactory {

    private static final String SELECT = "select";

    private static final String SELECT_EXPR = "selectExpr";

    private static final String FILTER = "filter";

    private static final String WHERE = "where";

    public static Dataset<Row> createDataFrame(SparkSession sparkSession, Dataset<Row> ancestorDataset, NodeData graphNode) {
        if (StringUtils.equals(graphNode.getType(), SELECT)) {
            List<String> vals = graphNode.getConfig().getStringList(SELECT, Collections.emptyList());
            if (vals.isEmpty()) {
                throw new IllegalArgumentException();
            } else if (vals.size() == 1) {
                ancestorDataset.select(vals.get(0));
            } else {
                return ancestorDataset.select(vals.get(0), listToArray(vals, 1));
            }
        }
        if (StringUtils.equals(graphNode.getType(), SELECT_EXPR)) {
            List<String> vals = graphNode.getConfig().getStringList(SELECT_EXPR, Collections.emptyList());
            if (vals.isEmpty()) {
                throw new IllegalArgumentException();
            } else {
                return ancestorDataset.selectExpr(listToArray(vals, 0));
            }
        }
        if (StringUtils.equals(graphNode.getType(), FILTER)) {
            String val = graphNode.getConfig().getStringVal(FILTER);
            if (StringUtils.isAnyEmpty(val)) {
                throw new IllegalArgumentException();
            } else {
                return ancestorDataset.filter(val);
            }
        }
        if (StringUtils.equals(graphNode.getType(), WHERE)) {
            String val = graphNode.getConfig().getStringVal(WHERE);
            if (StringUtils.isAnyEmpty(val)) {
                throw new IllegalArgumentException();
            } else {
                return ancestorDataset.where(val);
            }
        }
        throw new IllegalArgumentException();
    }

    private static String[] listToArray(List<String> list, int index) {
        String[] ret = new String[list.size() - index];
        for (int i = 0; i < list.size() - index; i++) {
            ret[0] = list.get(i + index);
        }
        return ret;
    }
}
