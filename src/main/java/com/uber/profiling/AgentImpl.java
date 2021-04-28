/*
 * Copyright (c) 2018 Uber Technologies, Inc.
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

package com.uber.profiling;

import com.uber.profiling.profilers.CpuAndMemoryProfiler;
import com.uber.profiling.profilers.IOProfiler;
import com.uber.profiling.profilers.MethodArgumentCollector;
import com.uber.profiling.profilers.MethodArgumentProfiler;
import com.uber.profiling.profilers.MethodDurationCollector;
import com.uber.profiling.profilers.MethodDurationProfiler;
import com.uber.profiling.profilers.ProcessInfoProfiler;
import com.uber.profiling.profilers.ThreadInfoProfiler;
import com.uber.profiling.profilers.StacktraceCollectorProfiler;
import com.uber.profiling.profilers.StacktraceReporterProfiler;
import com.uber.profiling.transformers.JavaAgentFileTransformer;
import com.uber.profiling.transformers.MethodProfilerStaticProxy;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.ClassAndMethodLongMetricBuffer;
import com.uber.profiling.util.ClassMethodArgumentMetricBuffer;
import com.uber.profiling.util.SparkUtils;
import com.uber.profiling.util.StacktraceMetricBuffer;
import java.lang.instrument.Instrumentation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AgentImpl {
    public static final String VERSION = "1.0.0";
    
    private static final AgentLogger logger = AgentLogger.getLogger(AgentImpl.class.getName());

    private static final int MAX_THREAD_POOL_SIZE = 2;

    private Map<String, Profiler> profilers = new ConcurrentHashMap<>();
    private String processUuid;
    private String appId;
    private JavaAgentFileTransformer transformer;
    private Thread shutdownHook;
    // use ScheduledThreadPoolExecutor instead of executor service
    // so we can set setRemoveOnCancelPolicy
    private ScheduledThreadPoolExecutor scheduledExecutorService;
    private boolean started = false;

    public void run(Arguments arguments, Instrumentation instrumentation, Collection<AutoCloseable> objectsToCloseOnShutdown) {
        if (arguments.isNoop()) {
            logger.info("Agent noop is true, do not run anything");
            return;
        }
        
        Reporter reporter = arguments.getReporter();

        if (processUuid == null) {
            processUuid = UUID.randomUUID().toString();
        }

        if (appId == null){
            String appIdVariable = arguments.getAppIdVariable();
            if (appIdVariable != null && !appIdVariable.isEmpty()) {
                appId = System.getenv(appIdVariable);
            }

            if (appId == null || appId.isEmpty()) {
                appId = SparkUtils.probeAppId(arguments.getAppIdRegex());
            }
        }

        if (transformer == null) {
            if (!arguments.getDurationProfiling().isEmpty()
                || !arguments.getArgumentProfiling().isEmpty()) {
                transformer = new JavaAgentFileTransformer(
                    arguments.getDurationProfiling(),
                    arguments.getArgumentProfiling());
                instrumentation.addTransformer(transformer);
            }
        }

        createProfilers(reporter, arguments, processUuid, appId);
        runProfilers();
        scheduleProfilers();

        // set set/update shutdown hook
        if (shutdownHook != null) {
            // cancel previous, in case new profilers are added at runtime
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
        shutdownHook = new Thread(new ShutdownHookRunner(profilers.values(),
            Arrays.asList(reporter), objectsToCloseOnShutdown));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Create or update the schedule interval for the profilers
     */
    private void createProfilers(Reporter reporter, Arguments arguments, String processUuid, String appId) {
        String tag = arguments.getTag();
        String cluster = arguments.getCluster();
        long metricInterval = arguments.getMetricInterval();

        // create once the profiler,
        // but update interval period in case this is changed at runtime
        profilers.computeIfAbsent(CpuAndMemoryProfiler.PROFILER_NAME, k -> {
            CpuAndMemoryProfiler cpuAndMemoryProfiler = new CpuAndMemoryProfiler(reporter);
            cpuAndMemoryProfiler.setTag(tag);
            cpuAndMemoryProfiler.setCluster(cluster);
            cpuAndMemoryProfiler.setProcessUuid(processUuid);
            cpuAndMemoryProfiler.setAppId(appId);
            return cpuAndMemoryProfiler;
        }).setIntervalMillis(metricInterval);

        profilers.computeIfAbsent(ProcessInfoProfiler.PROFILER_NAME, k -> {
            ProcessInfoProfiler processInfoProfiler = new ProcessInfoProfiler(reporter);
            processInfoProfiler.setTag(tag);
            processInfoProfiler.setCluster(cluster);
            processInfoProfiler.setProcessUuid(processUuid);
            processInfoProfiler.setAppId(appId);
            processInfoProfiler.setIntervalMillis(-1);
            return processInfoProfiler;
        });

        if (arguments.isThreadProfiling()) {
            ThreadInfoProfiler threadInfoProfiler = new ThreadInfoProfiler(reporter);
            threadInfoProfiler.setTag(tag);
            threadInfoProfiler.setCluster(cluster);
            threadInfoProfiler.setIntervalMillis(metricInterval);
            threadInfoProfiler.setProcessUuid(processUuid);
            threadInfoProfiler.setAppId(appId);

            profilers.add(threadInfoProfiler);
        }

        if (!arguments.getDurationProfiling().isEmpty()) {
            profilers.computeIfAbsent(MethodDurationProfiler.PROFILER_NAME, k -> {
                ClassAndMethodLongMetricBuffer classAndMethodMetricBuffer = new ClassAndMethodLongMetricBuffer();

                MethodDurationProfiler methodDurationProfiler = new MethodDurationProfiler(
                    classAndMethodMetricBuffer, reporter);
                methodDurationProfiler.setTag(tag);
                methodDurationProfiler.setCluster(cluster);
                methodDurationProfiler.setProcessUuid(processUuid);
                methodDurationProfiler.setAppId(appId);

                MethodDurationCollector methodDurationCollector = new MethodDurationCollector(
                    classAndMethodMetricBuffer);
                MethodProfilerStaticProxy.setCollector(methodDurationCollector);
                return methodDurationProfiler;
            }).setIntervalMillis(metricInterval);
        }

        if (!arguments.getArgumentProfiling().isEmpty()) {
            profilers.computeIfAbsent(MethodArgumentCollector.PROFILER_NAME, k -> {
                ClassMethodArgumentMetricBuffer classAndMethodArgumentBuffer = new ClassMethodArgumentMetricBuffer();

                MethodArgumentProfiler methodArgumentProfiler = new MethodArgumentProfiler(
                    classAndMethodArgumentBuffer, reporter);
                methodArgumentProfiler.setTag(tag);
                methodArgumentProfiler.setCluster(cluster);
                methodArgumentProfiler.setProcessUuid(processUuid);
                methodArgumentProfiler.setAppId(appId);

                MethodArgumentCollector methodArgumentCollector = new MethodArgumentCollector(
                    classAndMethodArgumentBuffer);
                MethodProfilerStaticProxy.setArgumentCollector(methodArgumentCollector);
                return methodArgumentProfiler;
            }).setIntervalMillis(metricInterval);

        }

        if (arguments.getSampleInterval() > 0) {
            Profiler reporterProfiler = profilers
                .computeIfAbsent(StacktraceReporterProfiler.PROFILER_NAME, k -> {
                    StacktraceMetricBuffer stacktraceMetricBuffer = new StacktraceMetricBuffer();
                    StacktraceReporterProfiler stacktraceReporterProfiler = new StacktraceReporterProfiler(
                        stacktraceMetricBuffer, reporter);
                    stacktraceReporterProfiler.setTag(tag);
                    stacktraceReporterProfiler.setCluster(cluster);
                    stacktraceReporterProfiler.setProcessUuid(processUuid);
                    stacktraceReporterProfiler.setAppId(appId);
                    return stacktraceReporterProfiler;
                });
            reporterProfiler.setIntervalMillis(metricInterval);

            profilers.computeIfAbsent(StacktraceCollectorProfiler.PROFILER_NAME, k -> {
                StacktraceCollectorProfiler stacktraceCollectorProfiler = new StacktraceCollectorProfiler(
                    ((StacktraceReporterProfiler) reporterProfiler).getBuffer(),
                    AgentThreadFactory.NAME_PREFIX);
                return stacktraceCollectorProfiler;
            }).setIntervalMillis(arguments.getSampleInterval());
        }

        if (arguments.isIoProfiling()) {
            profilers.computeIfAbsent(IOProfiler.PROFILER_NAME, k -> {
                IOProfiler ioProfiler = new IOProfiler(reporter);
                ioProfiler.setTag(tag);
                ioProfiler.setCluster(cluster);
                ioProfiler.setProcessUuid(processUuid);
                ioProfiler.setAppId(appId);

                return ioProfiler;
            }).setIntervalMillis(metricInterval);
        }
    }

    /**
     * Run profilers at start-up, once
     */
    private void runProfilers() {
        if (started) {
            logger.info("Profilers already started, do not run them again");
            return;
        }
        for (Profiler profiler : profilers.values()) {
            try {
                profiler.profile();
                logger.info("Ran profiler: " + profiler);
            } catch (Throwable ex) {
                logger.warn("Failed to run profiler: " + profiler, ex);
            }
        }
        started = true;
    }

    /**
     * (Re)schedule periodic profilers
     */
    private void scheduleProfilers() {
        if (scheduledExecutorService == null) {
            int threadPoolSize = Math.min(profilers.size(), MAX_THREAD_POOL_SIZE);
            scheduledExecutorService = new ScheduledThreadPoolExecutor(threadPoolSize, new AgentThreadFactory());
            scheduledExecutorService.setRemoveOnCancelPolicy(true);
        }

        for (Profiler profiler : profilers.values()) {
            if (profiler.isRunning()) {
                //cancel previous task if already scheduled
                profiler.cancel();
            }
            if (profiler.getIntervalMillis() <= 0){
                //one time profiler, don't schedule
                continue;
            }
            if (profiler.getIntervalMillis() < Arguments.MIN_INTERVAL_MILLIS) {
                throw new RuntimeException("Interval too short for profiler: " + profiler + ", must be at least " + Arguments.MIN_INTERVAL_MILLIS);
            }
            ScheduledFuture<?> handler = scheduledExecutorService.scheduleAtFixedRate(profiler, 0, profiler.getIntervalMillis(), TimeUnit.MILLISECONDS);
            // save the scheduled handler. so we can cancel if needed
            profiler.setScheduleHandler(handler);
            logger.info(String.format("Scheduled profiler %s with interval %s millis", profiler, profiler.getIntervalMillis()));
        }
    }
}
