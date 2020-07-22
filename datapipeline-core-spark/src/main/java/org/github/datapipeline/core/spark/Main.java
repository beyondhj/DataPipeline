package org.github.datapipeline.core.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.github.datapipeline.core.spark.batch.SparkBatchJob;
import org.github.datapipeline.core.spark.config.JobConfig;
import org.github.datapipeline.core.spark.config.SettingConfig;
import org.github.datapipeline.core.spark.options.JobOptions;
import org.github.datapipeline.core.spark.options.JobOptionsParser;
import org.github.datapipeline.core.spark.stream.SparkStreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        JobOptions jobOptions = JobOptionsParser.parse(args).getJobOptions();
        JobConfig jobConfig = JobConfig.parse(jobOptions.getJobJson());
        SparkSession sparkSession = createSparkSession(jobConfig.getSettingConfig());
        try {
            if (StringUtils.equals(jobConfig.getJobType(), SparkBatchJob.BATCH)) {
                SparkBatchJob.run(sparkSession, jobConfig);
            } else if (StringUtils.equals(jobConfig.getJobType(), SparkStreamJob.STREAM)) {
                SparkStreamJob.run(sparkSession, jobConfig);
            }
        } finally {
            sparkSession.close();
        }
    }

    private static SparkSession createSparkSession(SettingConfig settingConfig) {
        SparkSession.Builder builder = SparkSession.builder();
        for (Map.Entry<String, Object> entry : settingConfig.getAll().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.equals(key, SettingConfig.APP_NAME)) {
                builder.appName((String) value);
            } else if (StringUtils.equals(key, SettingConfig.MASTER)) {
                builder.master((String) value);
            } else if (value instanceof String) {
                builder.config(key, (String) value);
            } else if (value instanceof Boolean) {
                builder.config(key, (Boolean) value);
            } else if (value instanceof Double) {
                builder.config(key, (Double) value);
            } else if (value instanceof Integer) {
                builder.config(key, (Integer) value);
            } else if (value instanceof Long) {
                builder.config(key, (Long) value);
            }
        }
        return builder.getOrCreate();
    }

}
