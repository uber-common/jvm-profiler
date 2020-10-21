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

import com.uber.profiling.reporters.util.BaseJdbcDao;

import java.util.Arrays;

public class CpuAndMemoryProfilerMetricDao extends BaseJdbcDao {

  public static final String PARTITION_KEY = "DAYOFMONTH(epochMillis)";

  public static final String[] PRIMARY_KEYS = new String[] {"epochMillis", "name", "host", "processUuid", "appId"};

  public static final String[] INDEX_COLUMNS = new String[] {"epochMillis", "name", "host", "processUuid", "appId", "tag", "role"};

  public static final String[] DATETIME_COLUMNS = new String[] {"epochMillis"};

  public static final String[] TEXT_COLUMNS = new String[] {};

  public CpuAndMemoryProfilerMetricDao(String jdbcDriverClass, String connectionString, String tableName) {
    super(
        jdbcDriverClass,
        connectionString,
        CpuAndMemoryProfilerMetric.class,
        tableName,
        PARTITION_KEY,
        Arrays.asList(PRIMARY_KEYS),
        Arrays.asList(INDEX_COLUMNS),
        Arrays.asList(DATETIME_COLUMNS),
        Arrays.asList(TEXT_COLUMNS));
  }
}
