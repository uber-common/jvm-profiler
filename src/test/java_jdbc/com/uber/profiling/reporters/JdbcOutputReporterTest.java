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

import com.uber.profiling.profilers.CpuAndMemoryProfiler;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOutputReporterTest {
  @Test
  public void test() throws IOException {
    File file = File.createTempFile("h2dbfile", ".db");
    file.deleteOnExit();

    String connectionString =
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MySQL", file.getAbsolutePath());
    String tableName = "test_cpuAndMemoryProfilerTable";

    JdbcOutputReporter reporter = new JdbcOutputReporter();

    Map<String, List<String>> arguments = new HashMap<>();
    arguments.put("driverClass", Arrays.asList("org.h2.Driver"));
    arguments.put("connectionString", Arrays.asList(connectionString));
    arguments.put("cpuAndMemoryProfilerTableName", Arrays.asList(tableName));
    arguments.put("tablePartition", Arrays.asList("false"));

    reporter.updateArguments(arguments);

    Map<String, Object> map = new HashMap<>();

    map.put("epochMillis", 1603833802000L);
    map.put("name", "process1");
    map.put("host", "host1");
    map.put("processUuid", "uuid1");
    map.put("appId", "app1");
    map.put("tag", null);
    map.put("role", "role1");
    map.put("processCpuLoad", 0.2);
    map.put("systemCpuLoad", 0.3);
    map.put("processCpuTime", 1001L);
    map.put("heapMemoryTotalUsed", 2002L);
    map.put("heapMemoryCommitted", 3003L);
    map.put("heapMemoryMax", 4004L);
    map.put("nonHeapMemoryCommitted", 5005L);
    map.put("nonHeapMemoryTotalUsed", 6006L);
    map.put("nonHeapMemoryMax", 7007L);
    map.put("vmRSS", 8001L);
    map.put("vmHWM", 8002L);
    map.put("vmSize", 8003L);
    map.put("vmPeak", 8004L);

    reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);
    reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);

    map.put("epochMillis", 1603833802001L);
    reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);

    reporter.close();

    try (CpuAndMemoryProfilerMetricDao dao = new CpuAndMemoryProfilerMetricDao("org.h2.Driver", connectionString, tableName)) {
      Assert.assertEquals(2, dao.getTotalCount());

      CpuAndMemoryProfilerMetric entity = dao.getByPrimaryKeys(
          Arrays.asList(1603833802001L, "process1", "host1", "uuid1", "app1"),
          CpuAndMemoryProfilerMetric.class);
      Assert.assertNotNull(entity);
      Assert.assertEquals(null, entity.getTag());
      Assert.assertEquals((Double)0.2, entity.getProcessCpuLoad());
      Assert.assertEquals((Double)0.3, entity.getSystemCpuLoad());
      Assert.assertEquals((Long)1001L, entity.getProcessCpuTime());
      Assert.assertEquals((Long)2002L, entity.getHeapMemoryTotalUsed());
      Assert.assertEquals((Long)3003L, entity.getHeapMemoryCommitted());
      Assert.assertEquals((Long)4004L, entity.getHeapMemoryMax());
      Assert.assertEquals((Long)5005L, entity.getNonHeapMemoryCommitted());
      Assert.assertEquals((Long)6006L, entity.getNonHeapMemoryTotalUsed());
      Assert.assertEquals((Long)7007L, entity.getNonHeapMemoryMax());
      Assert.assertEquals((Long)8001L, entity.getVmRSS());
      Assert.assertEquals((Long)8002L, entity.getVmHWM());
      Assert.assertEquals((Long)8003L, entity.getVmSize());
      Assert.assertEquals((Long)8004L, entity.getVmPeak());
    }
  }
}
