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

package org.github.datapipeline.core.flink.options;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.Charsets;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * The Parser of Launcher commandline options
 */
public class JobOptionsParser {

    private final static String OPTION_JOB = "job";

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private JobOptions jobOptions = new JobOptions();

    public static JobOptionsParser parse(String[] args) throws ParseException, IOException {
        JobOptionsParser jobOptionsParser = new JobOptionsParser();
        jobOptionsParser.initOptions(jobOptionsParser.addOptions(args));
        return jobOptionsParser;
    }

    private CommandLine addOptions(String[] args) throws ParseException {
        options.addOption("job", true, "Job config");
        CommandLine cl = parser.parse(options, args);
        return cl;
    }

    private void initOptions(CommandLine cl) throws IOException {
        String value = cl.getOptionValue("job");
        jobOptions.setJob(value);
        jobOptions.setJobJson(readJobJson(value));
    }

    public JobOptions getJobOptions() {
        return jobOptions;
    }

    private String readJobJson(String filePath) throws IOException {
        File file = new File(filePath);
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] filecontent = new byte[(int) file.length()];
            in.read(filecontent);
            return new String(filecontent, Charsets.UTF_8.name());
        }
    }

    private void printUsage() {
        System.out.print(options.toString());
    }

}
