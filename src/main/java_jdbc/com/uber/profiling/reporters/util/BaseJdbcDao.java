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
import com.uber.profiling.util.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** * This class provides a framework to automatically create db table and insert data. */
public abstract class BaseJdbcDao implements AutoCloseable {
  private static final AgentLogger logger = AgentLogger.getLogger(BaseJdbcDao.class.getName());

  private final DbConnectionProvider connectionProvider;

  private final Class<?> entityClass;

  private final String tableName;

  private final SingleTableJdbcWriter singleTableJdbcWriter;

  private final String partitionKey;

  private final List<String> primaryKeys;
  private final List<String> timestampColumns;
  private final List<String> indexColumns;
  private final List<String> textColumns;

  public BaseJdbcDao(
      String jdbcDriverClass,
      String connectionString,
      Class<?> entityClass,
      String tableName,
      String partitionKey,
      Collection<String> primaryKeys,
      Collection<String> indexColumns,
      Collection<String> timestampColumns,
      Collection<String> textColumns) {
    connectionProvider = new DbConnectionProvider(jdbcDriverClass, connectionString);
    this.entityClass = entityClass;
    this.tableName = tableName;
    this.partitionKey = partitionKey;
    this.primaryKeys = new ArrayList<>(primaryKeys);
    this.indexColumns = new ArrayList<>(indexColumns);
    this.timestampColumns = new ArrayList<>(timestampColumns);
    this.textColumns = new ArrayList<>(textColumns);

    this.singleTableJdbcWriter =
        new SingleTableJdbcWriter(
            connectionProvider, tableName, new ArrayList<>(), timestampColumns, textColumns);
  }

  public String getJdbcConnectionString() {
    return singleTableJdbcWriter.getJdbcConnectionString();
  }

  public String getTableName() {
    return tableName;
  }

  public void createTable() {
    createTable(true);
  }

  public void createTable(boolean usePartition) {
    String paritionKey = usePartition ? partitionKey : null;
    String sql =
        JdbcUtils.getCreateTableSql(
            entityClass,
            getTableName(),
            paritionKey,
            primaryKeys,
            indexColumns,
            timestampColumns,
            textColumns);
    if (getJdbcConnectionString().toLowerCase().startsWith("jdbc:h2:")) {
      sql =
          JdbcUtils.getCreateTableSql(
              entityClass,
              getTableName(),
              null,
              primaryKeys,
              indexColumns,
              timestampColumns,
              textColumns,
              getTableName() + "_");
    }
    try (Connection con = getConnection()) {
      logger.info("Running sql: " + sql);
      PreparedStatement stmt = con.prepareStatement(sql);
      stmt.executeUpdate();
      logger.info("Finished sql: " + sql);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to create table: %s", this.getTableName()), e);
    }
  }

  public void insertOrUpdate(Object object) {
    singleTableJdbcWriter.write(object);
  }

  public List<List<Object>> queryColumns(int maxResultCount, String... columns) {
    String sql =
        String.format(
            "SELECT %s FROM %s LIMIT %d",
            StringUtils.join(columns, ", "), getTableName(), maxResultCount);
    logger.info("Running sql: " + sql);
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      ResultSet resultSet = stmt.executeQuery();
      List<List<Object>> result = new ArrayList<List<Object>>();
      while (resultSet.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
          row.add(resultSet.getObject(i + 1));
        }
        result.add(row);
      }
      logger.info("Finished sql: " + sql);
      return result;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to execute sql: " + sql, e);
    }
  }

  public <T> T getByPrimaryKey(Object primaryKeyValue, Class<T> clazz) {
    if (primaryKeys.size() > 1) {
      throw new RuntimeException(
          String.format(
              "Invalid primary key value, there are %s primary keys, but only one value is provided",
              primaryKeys.size()));
    }

    T result = null;

    String primaryKeyName = primaryKeys.get(0);
    String sql =
        String.format("SELECT * FROM %s WHERE %s = ? LIMIT 1", getTableName(), primaryKeyName);
    logger.info("Running sql: " + sql);
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setObject(1, primaryKeyValue);

      ResultSet resultSet = stmt.executeQuery();
      List<T> objects = createObjetsFromResultSet(resultSet, sql, clazz);
      if (objects.size() > 0) {
        result = objects.get(0);
      }

      logger.info("Finished sql: " + sql);
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  public <T> T getByPrimaryKeys(List<?> primaryKeyValue, Class<T> clazz) {
    if (primaryKeys.size() == 0) {
      throw new RuntimeException(
          String.format(
              "Invalid primary key value, there are %s primary keys", primaryKeys.size()));
    }

    T result = null;

    List<String> expressions = new ArrayList<>();
    for (int i = 0; i < primaryKeyValue.size(); i++) {
      expressions.add(String.format("%s=?", primaryKeys.get(i)));
    }
    String sql =
        String.format(
            "SELECT * FROM %s WHERE %s LIMIT 1",
            getTableName(), StringUtils.join(expressions, " AND "));
    logger.info("Running sql: " + sql);
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      for (int i = 0; i < primaryKeyValue.size(); i++) {
        Object obj = primaryKeyValue.get(i);
        if (this.timestampColumns.contains(primaryKeys.get(i))) {
          obj = JdbcUtils.getSqlTimestamp(obj);
        }
        stmt.setObject(i + 1, obj);
      }

      ResultSet resultSet = stmt.executeQuery();
      List<T> objects = createObjetsFromResultSet(resultSet, sql, clazz);
      if (objects.size() > 0) {
        result = objects.get(0);
      }

      logger.info("Finished sql: " + sql);
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  public long getTotalCount() {
    String sql = String.format("select count(*) from %s", getTableName());
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      ResultSet resultSet = stmt.executeQuery();
      if (!resultSet.next()) {
        throw new RuntimeException("Invalid result (empty result) from sql: " + sql);
      }
      return resultSet.getLong(1);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute query sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  public long executeUpdate(String sql) {
    final long result;
    try (Connection con = getConnection()) {
      logger.info("Running sql: " + sql);
      try (Statement stmt = con.createStatement()) {
        result = stmt.executeUpdate(sql);
      }
      logger.info("Finished sql: " + sql);
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute update sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  public List<List<Object>> executeQuery(String sql) {
    List<List<Object>> result = new ArrayList<>();
    try (Connection con = getConnection()) {
      logger.info("Running sql: " + sql);
      try (Statement stmt = con.createStatement()) {
        try (ResultSet resultSet = stmt.executeQuery(sql)) {
          while (resultSet.next()) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
              Object value = resultSet.getObject(i + 1);
              values.add(value);
            }
            result.add(values);
          }
          logger.info("Finished sql: " + sql);
          return result;
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute query sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  public String getDeleteByPartitionSql(int partitionNum) {
    String sql =
        String.format(
            "DELETE FROM %s PARTITION(p%s) where %s = %s",
            getTableName(), partitionNum, partitionKey, partitionNum);
    return sql;
  }

  public long deleteByPartition(int partitionNum) {
    String sql = getDeleteByPartitionSql(partitionNum);
    return executeUpdate(sql);
  }

  public long deleteByDateRange(Date startDayInclusive, Date endDayInclusive, boolean useParition) {
    if (useParition) {
      Date day = new Date(DateTimeUtils.truncateToDay(startDayInclusive.getTime()));
      long totalDeleteCount = 0;
      for (; day.getTime() <= endDayInclusive.getTime(); day = DateTimeUtils.addDays(day, 1)) {
        int dayOfMonth = DateTimeUtils.getDayOfMonth(day);
        long rowCount = deleteByPartition(dayOfMonth);
        totalDeleteCount += rowCount;
        logger.info(String.format("Deleted data of day %s containing %s rows", dayOfMonth, rowCount));
      }
      return totalDeleteCount;
    } else {
      Date nextDay = DateTimeUtils.addDays(endDayInclusive, 1);
      Date endDayExclusive = DateTimeUtils.truncateToDay(nextDay);
      String sql =
          String.format(
              "DELETE FROM %s where timestamp >= '%s' and timestamp < '%s'",
              getTableName(),
              DateTimeUtils.formatAsIsoWithoutMillis(startDayInclusive),
              DateTimeUtils.formatAsIsoWithoutMillis(endDayExclusive));
      logger.info(String.format("Running sql to delete data: %s", sql));
      long totalDeleteCount = executeUpdate(sql);
      return totalDeleteCount;
    }
  }

  @Override
  public void close() {
    connectionProvider.close();
  }

  protected Connection getConnection() {
    return connectionProvider.getConnection();
  }

  protected <T> List<T> executeStatement(PreparedStatement stmt, String sql, Class<T> clazz) {
    try {
      logger.info("Running sql: " + sql);
      try (ResultSet resultSet = stmt.executeQuery(sql)) {
        List<T> result = createObjetsFromResultSet(resultSet, sql, clazz);
        logger.info("Finished sql: " + sql);
        return result;
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to execute query sql: %s, %s", sql, e.getMessage()), e);
    }
  }

  protected <T> List<T> createObjetsFromResultSet(ResultSet resultSet, String sql, Class<T> clazz) {
    List<T> result = new ArrayList<>();

    Map<String, PropertyDescriptor> propertyDescriptorMap =
        BeanUtils.getPropertyDescriptorMapWithLowerCaseName(clazz);

    try {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      List<String> columnNames = new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        String columnName = resultSetMetaData.getColumnName(i + 1);
        columnNames.add(columnName);
      }

      while (resultSet.next()) {
        T entity;
        try {
          entity = (T) clazz.getConstructor().newInstance();
        } catch (Throwable e) {
          throw new RuntimeException("Failed to create object from class " + clazz);
        }

        for (int i = 0; i < columnCount; i++) {
          Object columnValue = resultSet.getObject(i + 1);
          if (columnValue == null) {
            continue;
          }
          String columnName = columnNames.get(i);
          PropertyDescriptor descriptor = propertyDescriptorMap.get(columnName.toLowerCase());
          if (descriptor == null) {
            logger.debug(
                String.format(
                    "Ignored column %s in jdbc result, because it does not exist in class %s",
                    columnName, clazz));
            continue;
          }
          writeJdbcValueToBean(entity, descriptor, columnValue);
        }

        result.add(entity);
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to convert result set of sql to %s objects: %s, %s",
              clazz.getName(), sql, e.getMessage()),
          e);
    }

    return result;
  }

  private void writeJdbcValueToBean(Object bean, PropertyDescriptor descriptor, Object jdbcValue) {
    if (jdbcValue == null) {
      try {
        descriptor.getWriteMethod().invoke(bean, jdbcValue);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(
            String.format(
                "Failed to write null jdbc value %s to bean %s on property %s",
                jdbcValue, bean, descriptor.getName()),
            e);
      }
      return;
    }

    if (jdbcValue instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) jdbcValue;
      writeDateValueToBean(timestamp, bean, descriptor);
    } else if (jdbcValue instanceof java.sql.Date) {
      java.sql.Date date = (java.sql.Date) jdbcValue;
      writeDateValueToBean(date, bean, descriptor);
    } else if (jdbcValue instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) jdbcValue;
      writeDateValueToBean(date, bean, descriptor);
    } else if (jdbcValue instanceof Clob) {
      Clob clob = (Clob) jdbcValue;
      try {
        byte[] bytes = IOUtils.toByteArray(clob.getAsciiStream());
        jdbcValue = new String(bytes, StandardCharsets.UTF_8);
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Failed to write jdbc value %s to bean %s on property %s",
                jdbcValue, bean, descriptor.getName()),
            e);
      }
      try {
        descriptor.getWriteMethod().invoke(bean, jdbcValue);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(
            String.format(
                "Failed to write jdbc value %s to bean %s on property %s",
                jdbcValue, bean, descriptor.getName()),
            e);
      }
    } else if (descriptor.getPropertyType().equals(Boolean.class)) {
      Boolean propertyValueToSet = null;
      if (jdbcValue != null) {
        if (jdbcValue.getClass().equals(Boolean.class)) {
          propertyValueToSet = (Boolean) jdbcValue;
        } else if (jdbcValue.getClass().equals(Byte.class)) {
          propertyValueToSet = ((Byte) jdbcValue) != 0;
        } else if (jdbcValue.getClass().equals(Short.class)) {
          propertyValueToSet = ((Short) jdbcValue) != 0;
        } else if (jdbcValue.getClass().equals(Integer.class)) {
          propertyValueToSet = ((Integer) jdbcValue) != 0;
        } else if (jdbcValue.getClass().equals(Long.class)) {
          propertyValueToSet = ((Long) jdbcValue) != 0;
        } else {
          throw new RuntimeException(
              String.format(
                  "Invalid jdbc value (%s, type: %s) for boolean bean property",
                  jdbcValue, jdbcValue.getClass()));
        }
      }
      try {
        descriptor.getWriteMethod().invoke(bean, propertyValueToSet);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(
            String.format(
                "Failed to write jdbc value %s to bean %s on boolean property %s",
                jdbcValue, bean, descriptor.getName()),
            e);
      }
    } else {
      try {
        descriptor.getWriteMethod().invoke(bean, jdbcValue);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(
            String.format(
                "Failed to write jdbc value %s to bean %s on property %s",
                jdbcValue, bean, descriptor.getName()),
            e);
      }
    }
  }

  private void writeDateValueToBean(
      java.util.Date date, Object bean, PropertyDescriptor descriptor) {
    Class<?> propertyType = descriptor.getPropertyType();
    final Object propertyValue;
    if (propertyType.equals(Long.class)) {
      propertyValue = Long.valueOf(date.getTime());
    } else if (propertyType.equals(Double.class)) {
      propertyValue = Double.valueOf(date.getTime() / 1000.0);
    } else {
      throw new RuntimeException(
          String.format(
              "Could not write jdbc value %s to bean %s on property %s, unsupported property type: %s",
              date, bean, descriptor.getName(), propertyType));
    }
    try {
      descriptor.getWriteMethod().invoke(bean, propertyValue);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          String.format(
              "Failed to write jdbc value %s to bean %s on property %s",
              date, bean, descriptor.getName()),
          e);
    }
  }
}
