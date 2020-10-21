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

import com.uber.profiling.util.JsonUtils;
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

    dao.insertOrUpdate(JsonUtils.serialize(entity1));

    CpuAndMemoryProfilerMetric entity2 = new CpuAndMemoryProfilerMetric();
    entity2.setEpochMillis(1535651091000L);
    entity2.setName("name02");
    entity2.setHost("host02");
    entity2.setProcessUuid("process02");
    entity2.setAppId("app02");
    entity2.setTag("tag02");
    entity2.setProcessCpuLoad(0.222);
    entity2.setHeapMemoryCommitted(2000L);

    dao.insertOrUpdate(JsonUtils.serialize(entity2));

    Assert.assertEquals(2, dao.getTotalCount());

    dao.close();
  }
}
