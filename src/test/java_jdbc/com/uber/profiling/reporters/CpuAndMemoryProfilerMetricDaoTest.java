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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CpuAndMemoryProfilerMetricDaoTest {
  @Test
  public void test() throws IOException {
    File file = File.createTempFile("h2dbfile", ".db");
    file.deleteOnExit();

    String connectionString =
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MySQL", file.getAbsolutePath());

    CpuAndMemoryProfilerMetricDao dao = new CpuAndMemoryProfilerMetricDao("org.h2.Driver", connectionString, "test_CpuAndMemoryProfilerMetricDao_table");
    dao.createTable();
    dao.queryColumns(1000, "*");

    CpuAndMemoryProfilerMetric entity1 = new CpuAndMemoryProfilerMetric();
    entity1.setEpochMillis(1535651091000L);
    entity1.setName("name01");
    entity1.setHost("host01");
    entity1.setProcessUuid("process01");
    entity1.setAppId("app01");
    entity1.setTag("tag01");
    entity1.setProcessCpuLoad(0.111);
    entity1.setHeapMemoryCommitted(1000L);

    Map<Object, Object> map1 = new HashMap<>();
    map1.put("epochMillis", entity1.getEpochMillis());
    map1.put("name", entity1.getName());
    map1.put("host", entity1.getHost());
    map1.put("processUuid", entity1.getProcessUuid());
    map1.put("appId", entity1.getAppId());
    map1.put("tag", entity1.getTag());
    map1.put("processCpuLoad", entity1.getProcessCpuLoad());
    map1.put("heapMemoryCommitted", entity1.getHeapMemoryCommitted());

    dao.insertOrUpdate(map1);

    CpuAndMemoryProfilerMetric entity2 = new CpuAndMemoryProfilerMetric();
    entity2.setEpochMillis(1535651091000L);
    entity2.setName("name02");
    entity2.setHost("host02");
    entity2.setProcessUuid("process02");
    entity2.setAppId("app02");
    entity2.setTag("tag02");
    entity2.setProcessCpuLoad(0.222);
    entity2.setHeapMemoryCommitted(2000L);

    Map<Object, Object> map2 = new HashMap<>();
    map2.put("epochMillis", entity2.getEpochMillis());
    map2.put("name", entity2.getName());
    map2.put("host", entity2.getHost());
    map2.put("processUuid", entity2.getProcessUuid());
    map2.put("appId", entity2.getAppId());
    map2.put("tag", entity2.getTag());
    map2.put("processCpuLoad", entity2.getProcessCpuLoad());
    map2.put("heapMemoryCommitted", entity2.getHeapMemoryCommitted());

    dao.insertOrUpdate(map2);

    Assert.assertEquals(2, dao.getTotalCount());

    CpuAndMemoryProfilerMetric readback1 = dao.getByPrimaryKeys(
        Arrays.asList(
            entity1.getEpochMillis(),
            entity1.getName(),
            entity1.getHost(),
            entity1.getProcessUuid(),
            entity1.getAppId()),
        CpuAndMemoryProfilerMetric.class);

    Assert.assertEquals(entity1.getEpochMillis(), readback1.getEpochMillis());
    Assert.assertEquals(entity1.getName(), readback1.getName());
    Assert.assertEquals(entity1.getHost(), readback1.getHost());
    Assert.assertEquals(entity1.getProcessUuid(), readback1.getProcessUuid());
    Assert.assertEquals(entity1.getAppId(), readback1.getAppId());
    Assert.assertEquals(entity1.getTag(), readback1.getTag());
    Assert.assertEquals(entity1.getProcessCpuLoad(), readback1.getProcessCpuLoad());
    Assert.assertEquals(entity1.getHeapMemoryCommitted(), readback1.getHeapMemoryCommitted());

    CpuAndMemoryProfilerMetric readback2 = dao.getByPrimaryKeys(
        Arrays.asList(
            entity2.getEpochMillis(),
            entity2.getName(),
            entity2.getHost(),
            entity2.getProcessUuid(),
            entity2.getAppId()),
        CpuAndMemoryProfilerMetric.class);

    Assert.assertEquals(entity2.getEpochMillis(), readback2.getEpochMillis());
    Assert.assertEquals(entity2.getName(), readback2.getName());
    Assert.assertEquals(entity2.getHost(), readback2.getHost());
    Assert.assertEquals(entity2.getProcessUuid(), readback2.getProcessUuid());
    Assert.assertEquals(entity2.getAppId(), readback2.getAppId());
    Assert.assertEquals(entity2.getTag(), readback2.getTag());
    Assert.assertEquals(entity2.getProcessCpuLoad(), readback2.getProcessCpuLoad());
    Assert.assertEquals(entity2.getHeapMemoryCommitted(), readback2.getHeapMemoryCommitted());

    dao.close();
  }
}
