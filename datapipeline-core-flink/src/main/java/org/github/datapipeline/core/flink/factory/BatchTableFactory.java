package org.github.datapipeline.core.flink.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.github.datapipeline.core.flink.config.NodeData;

import java.util.Collections;
import java.util.List;

public class BatchTableFactory {

    private static final String SELECT = "select";

    private static final String FILTER = "filter";

    private static final String WHERE = "where";

    public static Table createTable(ExecutionEnvironment evn, Table ancestorTable, NodeData graphNode) {
        if (StringUtils.equals(graphNode.getType(), SELECT)) {
            List<String> vals = graphNode.getConfig().getStringList(SELECT, Collections.emptyList());
            return ancestorTable.select(parseExpressionList(vals));
        }
        if (StringUtils.equals(graphNode.getType(), FILTER)) {
            String val = graphNode.getConfig().getStringVal(FILTER);
            if (StringUtils.isAnyEmpty(val)) {
                throw new IllegalArgumentException();
            } else {
                return ancestorTable.filter(parseExpression(val));
            }
        }
        if (StringUtils.equals(graphNode.getType(), WHERE)) {
            String val = graphNode.getConfig().getStringVal(WHERE);
            if (StringUtils.isAnyEmpty(val)) {
                throw new IllegalArgumentException();
            } else {
                return ancestorTable.where(parseExpression(val));
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

    private static Expression[] parseExpressionList(List<String> fields) {
        return null;
    }

    private static Expression parseExpression(String predicate) {
        return null;
    }
}
