/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.github.datapipeline.core.flink.config;

import java.util.Map;

/**
 * The configuration of setting optoins
 */
public class SettingConfig extends AbstractConfig {

    public static final String JOB_NAME = "jobName";

    public static final String BUFFER_TIMEOUT = "bufferTimeout";

    public static final String MAX_PARALLELISM = "maxParallelism";

    public static final String PARALLELISM = "parallelism";

    public static final String RESTART_STRATEGY = "restartStrategy";

    public static final String STATE_BACKEND = "stateBackend";

    public static final String TIME_CHARACTERISTIC = "timeCharacteristic";

    public SettingConfig(Map<String, Object> map) {
        super(map);
    }

    public String getJobName() {
        return super.getStringVal(JOB_NAME);
    }

}

