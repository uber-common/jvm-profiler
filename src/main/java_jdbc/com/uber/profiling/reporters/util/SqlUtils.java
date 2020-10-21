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

package com.uber.profiling.reporters.util;

import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlUtils {
  private static final AgentLogger logger = AgentLogger.getLogger(SqlUtils.class.getName());

  public static void printResultSet(ResultSet resultSet) {
    printResultSetColumns(resultSet);
    printResultSetRows(resultSet);
  }

  public static void printResultSetColumns(ResultSet resultSet) {
    try {
      List<String> columnNames = new ArrayList<>();
      List<String> columnTypes = new ArrayList<>();
      for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
        columnNames.add(resultSet.getMetaData().getColumnName(i + 1));
        columnTypes.add(resultSet.getMetaData().getColumnTypeName(i + 1));
      }
      logger.info("Column names: " + StringUtils.join(columnNames, ", "));
      logger.info("Column types: " + StringUtils.join(columnTypes, ", "));
    } catch (SQLException e) {
      logger.warn("Failed to print sql result", e);
    }
  }

  public static void printResultSetCurrentRow(ResultSet resultSet) {
    try {
      List<String> rowValues = new ArrayList<>();
      for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
        rowValues.add(String.valueOf(resultSet.getObject(i + 1)));
      }
      logger.info("Row: " + StringUtils.join(rowValues, ", "));
    } catch (SQLException e) {
      logger.warn("Failed to print sql result", e);
    }
  }

  public static void printResultSetRows(ResultSet resultSet) {
    try {
      while (resultSet.next()) {
        List<String> rowValues = new ArrayList<>();
        for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
          rowValues.add(String.valueOf(resultSet.getObject(i + 1)));
        }
        logger.info("Row: " + StringUtils.join(rowValues, ", "));
      }
    } catch (SQLException e) {
      logger.warn("Failed to print sql result", e);
    }
  }
}
