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
import com.uber.profiling.reporters.util.DateTimeUtils;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.JsonUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JdbcOutputReporter implements Reporter {
    public static final String ARG_DRIVER_CLASS = "driverClass";
    public static final String ARG_CONNECTION_STRING = "connectionString";
    public static final String ARG_CPU_AND_MEMORY_PROFILER_TABLE_NAME = "cpuAndMemoryProfilerTableName";
    public static final String ARG_TABLE_PARTITION = "tablePartition";
    public static final String ARG_DATA_RETENTION = "dataRetention"; // in seconds
    public static final String ARG_DATA_CLEANUP_INTERVAL = "dataCleanupInterval"; // in seconds

    private static final AgentLogger logger = AgentLogger.getLogger(JdbcOutputReporter.class.getName());

    private String driverClass = "com.mysql.jdbc.Driver";
    private String connectionString;
    private String cpuAndMemoryProfilerTableName = "cpuAndMemoryProfiler";
    private long dataRetentionMillis = TimeUnit.DAYS.toMillis(7);
    private long dataCleanupIntervalMillis = TimeUnit.HOURS.toMillis(1);
    private boolean useTablePartition = true;

    private Object cpuAndMemoryProfilerMetricDaoLock = new Object();
    private CpuAndMemoryProfilerMetricDao cpuAndMemoryProfilerMetricDao;

    private long lastDataCleanupTime = 0;

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        if (profilerName.equals(CpuAndMemoryProfiler.PROFILER_NAME)) {
            CpuAndMemoryProfilerMetric cpuAndMemoryProfilerMetric = new CpuAndMemoryProfilerMetric();
            cpuAndMemoryProfilerMetric.setEpochMillis((Long)metrics.get("epochMillis"));
            cpuAndMemoryProfilerMetric.setName((String)metrics.get("name"));
            cpuAndMemoryProfilerMetric.setHost((String)metrics.get("host"));
            cpuAndMemoryProfilerMetric.setProcessUuid((String)metrics.get("processUuid"));
            cpuAndMemoryProfilerMetric.setAppId((String)metrics.get("appId"));
            cpuAndMemoryProfilerMetric.setTag((String)metrics.get("tag"));
            cpuAndMemoryProfilerMetric.setRole((String)metrics.get("role"));
            cpuAndMemoryProfilerMetric.setProcessCpuLoad((Double)metrics.get("processCpuLoad"));
            cpuAndMemoryProfilerMetric.setSystemCpuLoad((Double)metrics.get("systemCpuLoad"));
            cpuAndMemoryProfilerMetric.setProcessCpuTime((Long)metrics.get("processCpuTime"));
            cpuAndMemoryProfilerMetric.setHeapMemoryTotalUsed((Long)metrics.get("heapMemoryTotalUsed"));
            cpuAndMemoryProfilerMetric.setHeapMemoryCommitted((Long)metrics.get("heapMemoryCommitted"));
            cpuAndMemoryProfilerMetric.setHeapMemoryMax((Long)metrics.get("heapMemoryMax"));
            cpuAndMemoryProfilerMetric.setNonHeapMemoryTotalUsed((Long)metrics.get("nonHeapMemoryTotalUsed"));
            cpuAndMemoryProfilerMetric.setNonHeapMemoryCommitted((Long)metrics.get("nonHeapMemoryCommitted"));
            cpuAndMemoryProfilerMetric.setNonHeapMemoryMax((Long)metrics.get("nonHeapMemoryMax"));
            cpuAndMemoryProfilerMetric.setVmRSS((Long)metrics.get("vmRSS"));
            cpuAndMemoryProfilerMetric.setVmHWM((Long)metrics.get("vmHWM"));
            cpuAndMemoryProfilerMetric.setVmSize((Long)metrics.get("vmSize"));
            cpuAndMemoryProfilerMetric.setVmPeak((Long)metrics.get("vmPeak"));

            synchronized (cpuAndMemoryProfilerMetricDaoLock) {
                try {
                    if (cpuAndMemoryProfilerMetricDao == null) {
                        cpuAndMemoryProfilerMetricDao = new CpuAndMemoryProfilerMetricDao(driverClass, connectionString, cpuAndMemoryProfilerTableName);
                    }
                    cpuAndMemoryProfilerMetricDao.insertOrUpdate(cpuAndMemoryProfilerMetric);

                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastDataCleanupTime >= dataCleanupIntervalMillis) {
                        int dataRetentionDays = (int)(dataRetentionMillis / TimeUnit.DAYS.toMillis(1));
                        Date expiredDataDate = DateTimeUtils.addDays(new Date(DateTimeUtils.truncateToDay(currentTime)), -dataRetentionDays);
                        cpuAndMemoryProfilerMetricDao.deleteByDateRange(expiredDataDate, expiredDataDate, useTablePartition);
                        lastDataCleanupTime = currentTime;
                    }
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
    public void updateArguments(Map<String, List<String>> arguments) {
        String driverClassArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_DRIVER_CLASS);
        if (ArgumentUtils.needToUpdateArg(driverClassArgValue)) {
            driverClass = driverClassArgValue;
            logger.info("Got argument value for driverClass: " + driverClass);
        }

        String connectionStringArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_CONNECTION_STRING);
        if (ArgumentUtils.needToUpdateArg(connectionStringArgValue)) {
            connectionString = connectionStringArgValue;
            logger.info("Got argument value for connectionString: " + connectionString);
        }

        String cpuAndMemoryProfilerTableNameArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_CPU_AND_MEMORY_PROFILER_TABLE_NAME);
        if (ArgumentUtils.needToUpdateArg(cpuAndMemoryProfilerTableNameArgValue)) {
            cpuAndMemoryProfilerTableName = cpuAndMemoryProfilerTableNameArgValue;
            logger.info("Got argument value for cpuAndMemoryProfilerTableName: " + cpuAndMemoryProfilerTableName);

            try (CpuAndMemoryProfilerMetricDao dao = new CpuAndMemoryProfilerMetricDao(driverClass, connectionString, cpuAndMemoryProfilerTableName)) {
                dao.createTable(true);
            }
        }

        String tablePartitionArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_TABLE_PARTITION);
        if (ArgumentUtils.needToUpdateArg(tablePartitionArgValue)) {
            useTablePartition = Boolean.parseBoolean(tablePartitionArgValue);
            logger.info("Got argument value for useTablePartition: " + useTablePartition);
        }

        String dataRetentionArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_DATA_RETENTION);
        if (ArgumentUtils.needToUpdateArg(dataRetentionArgValue)) {
            dataRetentionMillis = Long.parseLong(dataRetentionArgValue) * 1000;
            logger.info("Got argument value for dataRetentionMillis: " + dataRetentionMillis);
        }

        String dataCleanupIntervalArgValue = ArgumentUtils.getArgumentSingleValue(arguments, ARG_DATA_CLEANUP_INTERVAL);
        if (ArgumentUtils.needToUpdateArg(dataCleanupIntervalArgValue)) {
            dataCleanupIntervalMillis = Long.parseLong(dataCleanupIntervalArgValue);
            logger.info("Got argument value for dataCleanupIntervalMillis: " + dataCleanupIntervalMillis);
        }

        try (CpuAndMemoryProfilerMetricDao dao = new CpuAndMemoryProfilerMetricDao(driverClass, connectionString, cpuAndMemoryProfilerTableName)) {
            dao.createTable(useTablePartition);
        }
    }
}

