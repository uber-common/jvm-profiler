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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** * This class writes a given object into db table. */
public class SingleTableJdbcWriter implements AutoCloseable {
  private static final AgentLogger logger = AgentLogger.getLogger(SingleTableJdbcWriter.class.getName());

  private static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }

  private DbConnectionProvider connectionProvider;
  private String tableName;

  private Set<String> ignoreColumnsLowerCase = new HashSet<>();
  private Set<String> timestampColumnsLowerCase = new HashSet<>();
  private Set<String> textColumnsLowerCase = new HashSet<>();

  private List<String> primaryKeys = new ArrayList<>();

  private List<String> columnNames = new ArrayList<>();
  private List<String> columnNamesLowerCase = new ArrayList<>();

  private Map<String, String> columnTypes = new HashMap<>();

  public SingleTableJdbcWriter(
      String jdbcDriverClass,
      String jdbcConnectionString,
      String tableName,
      Collection<String> ignoreColumns,
      Collection<String> timestampColumns,
      Collection<String> textColumns) {
    this(
        new DbConnectionProvider(jdbcDriverClass, jdbcConnectionString),
        tableName,
        ignoreColumns,
        timestampColumns,
        textColumns);
  }

  public SingleTableJdbcWriter(
      DbConnectionProvider DbConnectionProvider,
      String tableName,
      Collection<String> ignoreColumns,
      Collection<String> timestampColumns,
      Collection<String> textColumns) {
    this.connectionProvider = DbConnectionProvider;
    this.tableName = tableName;

    for (String entry : ignoreColumns) {
      this.ignoreColumnsLowerCase.add(entry.toLowerCase());
      logger.info("Will ignore column: " + entry);
    }

    if (timestampColumns != null) {
      for (String entry : timestampColumns) {
        this.timestampColumnsLowerCase.add(entry.toLowerCase());
        logger.info("Has timestamp column: " + entry);
      }
    }

    if (textColumns != null) {
      for (String entry : textColumns) {
        this.textColumnsLowerCase.add(entry.toLowerCase());
        logger.info("Has text column: " + entry);
      }
    }
  }

  public void initialize() {
    this.getTableMetadata();
  }

  public String getJdbcConnectionString() {
    return connectionProvider.getConnectionString();
  }

  public void write(Object object) {
    if (object == null) {
      logger.warn("Ignored null object");
      return;
    }

    if (this.columnNames.isEmpty()) {
      this.initialize();
    }

    Map<?, ?> values;

    if (object instanceof byte[]) {
      byte[] bytes = (byte[]) object;
      String json = new String(bytes, StandardCharsets.UTF_8);
      values = parseJson(json);
    } else if (object instanceof String) {
      String json = (String) object;
      values = parseJson(json);
    } else if (object instanceof Map) {
      values = (Map)object;
    } else {
      throw new RuntimeException("Unsupported output object: " + object);
    }

    List<String> nonNullFields = new ArrayList<>();
    List<Object> nonNullValues = new ArrayList<>();
    for (Map.Entry entry : values.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      String fieldName = entry.getKey().toString();
      if (ignoreColumnsLowerCase.contains(fieldName.toLowerCase())) {
        continue;
      }
      nonNullFields.add(fieldName);
      nonNullValues.add(entry.getValue());
    }

    List<String> updateList = new ArrayList<>();
    for (int i = 0; i < nonNullFields.size(); i++) {
      updateList.add(String.format("%s=?", nonNullFields.get(i)));
    }

    if (nonNullFields.isEmpty()) {
      throw new RuntimeException("Not support empty json or json with all values being null");
    }

    String sql =
        String.format(
            "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
            tableName,
            StringUtils.join(nonNullFields, ", "),
            StringUtils.join(Collections.nCopies(nonNullFields.size(), "?"), ", "),
            StringUtils.join(updateList, ", "));
    logger.info("Running sql: " + sql);
    Connection connection = connectionProvider.getConnection();
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      for (int i = 0; i < nonNullValues.size(); i++) {
        if (timestampColumnsLowerCase.contains(nonNullFields.get(i).toLowerCase())) {
          stmt.setTimestamp(i + 1, JdbcUtils.getSqlTimestamp(nonNullValues.get(i)));
        } else if (textColumnsLowerCase.contains(nonNullFields.get(i).toLowerCase())) {
          Clob clob = connection.createClob();
          clob.setString(1, nonNullValues.get(i).toString());
          stmt.setClob(i + 1, clob);
        } else {
          stmt.setObject(i + 1, nonNullValues.get(i));
        }
      }
      for (int i = 0; i < nonNullValues.size(); i++) {
        if (timestampColumnsLowerCase.contains(nonNullFields.get(i).toLowerCase())) {
          stmt.setTimestamp(
              nonNullValues.size() + i + 1, JdbcUtils.getSqlTimestamp(nonNullValues.get(i)));
        } else if (textColumnsLowerCase.contains(nonNullFields.get(i).toLowerCase())) {
          Clob clob = connection.createClob();
          clob.setString(1, nonNullValues.get(i).toString());
          stmt.setClob(nonNullValues.size() + i + 1, clob);
        } else {
          stmt.setObject(nonNullValues.size() + i + 1, nonNullValues.get(i));
        }
      }
      stmt.executeUpdate();
      logger.info("Finished sql: " + sql);
    } catch (Throwable e) {
      connectionProvider.close();
      throw new RuntimeException(
          String.format(
              "Failed to run sql [%s] to insert object %s: %s",
              sql, object, ExceptionUtils.getStackTrace(e)),
          e);
    }
  }

  @Override
  public void close() {
    if (connectionProvider == null) {
      return;
    }

    connectionProvider.close();
  }

  private Map<?, ?> parseJson(String json) {
    Map<?, ?> values;
    try {
      values = mapper.readValue(json, Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse json object: " + json, e);
    }
    return values;
  }

  private void getTableMetadata() {
    columnNames.clear();
    columnNamesLowerCase.clear();
    columnTypes.clear();

    Connection con = connectionProvider.getConnection();
    try {
      DatabaseMetaData metaData = con.getMetaData();

      String matchedDbName = null;

      ResultSet getTablesResultSet = metaData.getTables(null, null, "%", null);
      while (getTablesResultSet.next()) {
        String table = getTablesResultSet.getString(3);
        logger.info("Checking table: " + table);
        if (this.tableName.equalsIgnoreCase(table)) {
          matchedDbName = table;
          ResultSet getColumnsResultSet = metaData.getColumns(null, null, table, null);
          while (getColumnsResultSet.next()) {
            String columnName = getColumnsResultSet.getString(4);
            String columnType = getColumnsResultSet.getString(6);
            columnNames.add(columnName);
            columnNamesLowerCase.add(columnName.toLowerCase());
            columnTypes.put(columnName.toLowerCase(), columnType);
            logger.info(String.format("Found column: %s, %s", columnName, columnType));
          }
        }
      }

      if (matchedDbName != null) {
        ResultSet getExportedKeysResultSet = metaData.getPrimaryKeys(null, null, tableName);
        while (getExportedKeysResultSet.next()) {
          String columnName = getExportedKeysResultSet.getString(4);
          primaryKeys.add(columnName);
          logger.info("Found primay key: " + columnName);
        }
      }

      if (!primaryKeys.isEmpty() && !columnNames.isEmpty()) {
        return;
      }

      ResultSet getCatalogsResult = metaData.getCatalogs();
      while (getCatalogsResult.next()) {
        String catalog = String.valueOf(getCatalogsResult.getObject(1));
        logger.info("Checking catalog: " + catalog);
        ResultSet getSchemasResult = metaData.getSchemas(catalog, null);
        SqlUtils.printResultSet(getSchemasResult);
        while (getSchemasResult.next()) {
          String schema = String.valueOf(getSchemasResult.getObject(1));
          logger.info("Checking schema: " + schema);
          ResultSet getTablesResult = metaData.getTables(catalog, schema, null, null);
          while (getTablesResult.next()) {
            String table = getTablesResult.getString(3);
            logger.info("Checking table: " + table);
            if (this.tableName.equalsIgnoreCase(table)) {
              if (matchedDbName == null) {
                matchedDbName = table;
              }
              if (columnNames.isEmpty()) {
                ResultSet getColumnsResultSet = metaData.getColumns(catalog, schema, table, null);
                while (getColumnsResultSet.next()) {
                  String columnName = getColumnsResultSet.getString(4);
                  String columnType = getColumnsResultSet.getString(6);
                  columnNames.add(columnName);
                  columnNamesLowerCase.add(columnName.toLowerCase());
                  columnTypes.put(columnName.toLowerCase(), columnType);
                  logger.info(String.format("Found column: %s, %s", columnName, columnType));
                }
              }
            }
          }
        }
      }

      if (matchedDbName != null && primaryKeys.isEmpty()) {
        ResultSet getExportedKeysResultSet = metaData.getPrimaryKeys(null, null, tableName);
        while (getExportedKeysResultSet.next()) {
          String columnName = getExportedKeysResultSet.getString(4);
          primaryKeys.add(columnName);
          logger.info("Found primay key: " + columnName);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to get meta data for %s table %s",
              connectionProvider.getConnectionString(), tableName),
          e);
    }
  }
}
