/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.profiling.reporters;

import com.uber.profiling.ArgumentUtils;
import com.uber.profiling.Reporter;
import com.uber.profiling.profilers.CpuAndMemoryProfiler;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.JsonUtils;

import java.util.List;
import java.util.Map;

public class JdbcOutputReporter implements Reporter {
    public static final String ARG_DRIVER_CLASS = "driverClass";
    public static final String ARG_CONNECTION_STRING = "connectionString";
    public static final String ARG_CPU_AND_MEMORY_PROFILER_TABLE_NAME = "cpuAndMemoryProfilerTableName";

    private static final AgentLogger logger = AgentLogger.getLogger(JdbcOutputReporter.class.getName());

    private String driverClass;
    private String connectionString;
    private String cpuAndMemoryProfilerTableName;

    private Object cpuAndMemoryProfilerMetricDaoLock = new Object();
    private CpuAndMemoryProfilerMetricDao cpuAndMemoryProfilerMetricDao;

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        if (profilerName.equals(CpuAndMemoryProfiler.PROFILER_NAME)) {
            synchronized (cpuAndMemoryProfilerMetricDaoLock) {
                try {
                    if (cpuAndMemoryProfilerMetricDao == null) {
                        cpuAndMemoryProfilerMetricDao = new CpuAndMemoryProfilerMetricDao(driverClass, connectionString, cpuAndMemoryProfilerTableName);
                    }
                    CpuAndMemoryProfilerMetric cpuAndMemoryProfilerMetric = new CpuAndMemoryProfilerMetric();
                    String jsonStr = JsonUtils.serialize(cpuAndMemoryProfilerMetric);
                    cpuAndMemoryProfilerMetricDao.insertOrUpdate(jsonStr);
                } catch (Throwable ex) {
                    logger.warn("Failed to insert metric to db table", ex);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (cpuAndMemoryProfilerMetricDaoLock) {
            if (cpuAndMemoryProfilerMetricDao != null) {
                cpuAndMemoryProfilerMetricDao.close();
                cpuAndMemoryProfilerMetricDao = null;
            }
        }
    }


    @Override
    public void updateArguments(Map<String, List<String>> connectionProperties) {
        String driverClassArgValue = ArgumentUtils.getArgumentSingleValue(connectionProperties, ARG_DRIVER_CLASS);
        if (ArgumentUtils.needToUpdateArg(driverClassArgValue)) {
            driverClass = driverClassArgValue;
            logger.info("Got argument value for driverClass: " + driverClass);
        }

        String connectionStringArgValue = ArgumentUtils.getArgumentSingleValue(connectionProperties, ARG_CONNECTION_STRING);
        if (ArgumentUtils.needToUpdateArg(connectionStringArgValue)) {
            connectionString = connectionStringArgValue;
            logger.info("Got argument value for connectionString: " + connectionString);
        }

        String cpuAndMemoryProfilerTableNameArgValue = ArgumentUtils.getArgumentSingleValue(connectionProperties, ARG_CPU_AND_MEMORY_PROFILER_TABLE_NAME);
        if (ArgumentUtils.needToUpdateArg(cpuAndMemoryProfilerTableNameArgValue)) {
            cpuAndMemoryProfilerTableName = cpuAndMemoryProfilerTableNameArgValue;
            logger.info("Got argument value for cpuAndMemoryProfilerTableName: " + cpuAndMemoryProfilerTableName);

            try (CpuAndMemoryProfilerMetricDao dao = new CpuAndMemoryProfilerMetricDao(driverClass, connectionString, cpuAndMemoryProfilerTableName)) {
                dao.createTable();
            }
        }
    }
}

