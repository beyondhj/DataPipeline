package org.github.datapipeline.core.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.github.datapipeline.core.flink.batch.FlinkBatchJob;
import org.github.datapipeline.core.flink.config.JobConfig;
import org.github.datapipeline.core.flink.config.SettingConfig;
import org.github.datapipeline.core.flink.options.JobOptions;
import org.github.datapipeline.core.flink.options.JobOptionsParser;
import org.github.datapipeline.core.flink.stream.FlinkStreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        JobOptions jobOptions = JobOptionsParser.parse(args).getJobOptions();
        JobConfig jobConfig = JobConfig.parse(jobOptions.getJobJson());
        if (StringUtils.equals(jobConfig.getJobType(), FlinkBatchJob.BATCH)) {
            ExecutionEnvironment env = createExecutionEnvironment(jobConfig.getSettingConfig());
            FlinkBatchJob.run(env, jobConfig);
        } else if (StringUtils.equals(jobConfig.getJobType(), FlinkStreamJob.STREAM)) {
            StreamExecutionEnvironment env = createStreamExecutionEnvironment(jobConfig.getSettingConfig());
            FlinkStreamJob.run(env, jobConfig);
        }
    }

    private static StreamExecutionEnvironment createStreamExecutionEnvironment(SettingConfig settingConfig) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        for (Map.Entry<String, Object> entry : settingConfig.getAll().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.equals(key, SettingConfig.BUFFER_TIMEOUT)) {
                env.setBufferTimeout((Long) value);
            } else if (StringUtils.equals(key, SettingConfig.MAX_PARALLELISM)) {
                env.setMaxParallelism((Integer) value);
            } else if (StringUtils.equals(key, SettingConfig.PARALLELISM)) {
                env.setParallelism((Integer) value);
            } else if (StringUtils.equals(key, SettingConfig.RESTART_STRATEGY)) {
                //TODO
                //env.setRestartStrategy();
            } else if (StringUtils.equals(key, SettingConfig.STATE_BACKEND)) {
                //TODO
                //env.setStateBackend()
            } else if (StringUtils.equals(key, SettingConfig.TIME_CHARACTERISTIC)) {
                //TODO
                //env.setStreamTimeCharacteristic();
            }
        }
        return env;
    }

    private static ExecutionEnvironment createExecutionEnvironment(SettingConfig settingConfig) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        for (Map.Entry<String, Object> entry : settingConfig.getAll().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.equals(key, SettingConfig.PARALLELISM)) {
                env.setParallelism((Integer) value);
            } else if (StringUtils.equals(key, SettingConfig.RESTART_STRATEGY)) {
                //TODO
                //env.setRestartStrategy();
            } else if (StringUtils.equals(key, SettingConfig.STATE_BACKEND)) {
                //TODO
                //env.setStateBackend()
            } else if (StringUtils.equals(key, SettingConfig.TIME_CHARACTERISTIC)) {
                //TODO
                //env.setStreamTimeCharacteristic();
            }
        }
        return env;
    }

}
