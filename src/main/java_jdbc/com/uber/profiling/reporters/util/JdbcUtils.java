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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class JdbcUtils {
  public static String getCreateTableSql(
      Class<?> clazz,
      String tableName,
      Collection<String> primaryKeys,
      Collection<String> indexFields,
      Collection<String> timestampFields,
      Collection<String> textFields) {
    return getCreateTableSql(
        clazz, tableName, null, primaryKeys, indexFields, timestampFields, textFields);
  }

  public static String getCreateTableSql(
      Class<?> clazz,
      String tableName,
      String partitionKey,
      Collection<String> primaryKeys,
      Collection<String> indexFields,
      Collection<String> timestampFields,
      Collection<String> textFields) {
    return getCreateTableSql(
        clazz,
        tableName,
        partitionKey,
        primaryKeys,
        indexFields,
        timestampFields,
        textFields,
        null);
  }

  public static String getCreateTableSql(
      Class<?> clazz,
      String tableName,
      String partitionKey,
      Collection<String> primaryKeys,
      Collection<String> indexFields,
      Collection<String> timestampFields,
      Collection<String> textFields,
      String indexNamePrefix) {
    if (textFields == null) {
      textFields = new ArrayList<>();
    }

    if (indexNamePrefix == null) {
      indexNamePrefix = "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("CREATE TABLE IF NOT EXISTS %s (", tableName));

    try {
      BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
      PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

      List<PropertyDescriptor> primaryKeyPropertyDescriptors = new ArrayList<>();
      List<PropertyDescriptor> indexFieldsPropertyDescriptors = new ArrayList<>();
      List<PropertyDescriptor> otherPropertyDescriptors = new ArrayList<>();

      for (PropertyDescriptor entry : propertyDescriptors) {
        if (entry.getReadMethod() == null || entry.getWriteMethod() == null) {
          continue;
        }

        if (primaryKeys.contains(entry.getName())) {
          primaryKeyPropertyDescriptors.add(entry);
        }
        if (indexFields.contains(entry.getName())) {
          indexFieldsPropertyDescriptors.add(entry);
        }
        if (!primaryKeys.contains(entry.getName()) && !indexFields.contains(entry.getName())) {
          otherPropertyDescriptors.add(entry);
        }
      }

      if (primaryKeyPropertyDescriptors.size() != primaryKeys.size()) {
        throw new RuntimeException(
            String.format(
                "Invalid primary keys. There are %s in bean object, but %s in input argument",
                primaryKeyPropertyDescriptors.size(), primaryKeys.size()));
      }

      List<PropertyDescriptor> primaryKeyPropertyDescriptorsWithCorrectOrder = new ArrayList<>();
      for (String key : primaryKeys) {
        Optional<PropertyDescriptor> propertyDescriptor =
            primaryKeyPropertyDescriptors.stream().filter(t -> t.getName().equals(key)).findFirst();
        if (!propertyDescriptor.isPresent()) {
          throw new RuntimeException("Did not find matching property for primary key: " + key);
        }
        primaryKeyPropertyDescriptorsWithCorrectOrder.add(propertyDescriptor.get());
      }
      primaryKeyPropertyDescriptors = primaryKeyPropertyDescriptorsWithCorrectOrder;

      if (indexFieldsPropertyDescriptors.size() != indexFields.size()) {
        throw new RuntimeException(
            String.format(
                "Invalid index fields. There are %s in bean object, but %s in input argument",
                indexFieldsPropertyDescriptors.size(), indexFields.size()));
      }

      List<PropertyDescriptor> allPropertyDescriptors = new ArrayList<>();

      allPropertyDescriptors.addAll(primaryKeyPropertyDescriptors);

      for (int i = 0; i < indexFieldsPropertyDescriptors.size(); i++) {
        String columnName = indexFieldsPropertyDescriptors.get(i).getName();
        if (primaryKeys.contains(columnName)) {
          continue;
        }
        allPropertyDescriptors.add(indexFieldsPropertyDescriptors.get(i));
      }

      allPropertyDescriptors.addAll(otherPropertyDescriptors);

      sb.append(allPropertyDescriptors.get(0).getName());
      sb.append(" ");
      sb.append(
          getJdbcTypeString(
              allPropertyDescriptors.get(0),
              primaryKeys.contains(allPropertyDescriptors.get(0).getName())
                  || indexFields.contains(allPropertyDescriptors.get(0).getName()),
              timestampFields.contains(allPropertyDescriptors.get(0).getName()),
              textFields.contains(allPropertyDescriptors.get(0).getName())));

      for (int i = 1; i < allPropertyDescriptors.size(); i++) {
        sb.append(", ");
        sb.append(allPropertyDescriptors.get(i).getName());
        sb.append(" ");
        sb.append(
            getJdbcTypeString(
                allPropertyDescriptors.get(i),
                primaryKeys.contains(allPropertyDescriptors.get(i).getName())
                    || indexFields.contains(allPropertyDescriptors.get(i).getName()),
                timestampFields.contains(allPropertyDescriptors.get(i).getName()),
                textFields.contains(allPropertyDescriptors.get(i).getName())));
      }

      if (!primaryKeyPropertyDescriptors.isEmpty()) {
        sb.append(", PRIMARY KEY(");
        sb.append(primaryKeyPropertyDescriptors.get(0).getName());
        for (int i = 1; i < primaryKeyPropertyDescriptors.size(); i++) {
          sb.append(", ");
          sb.append(primaryKeyPropertyDescriptors.get(i).getName());
        }
        sb.append(")");
      }

      for (int i = 0; i < indexFieldsPropertyDescriptors.size(); i++) {
        sb.append(
            String.format(
                ", INDEX %sindex_%s (%s)",
                indexNamePrefix,
                indexFieldsPropertyDescriptors.get(i).getName(),
                indexFieldsPropertyDescriptors.get(i).getName()));
      }

      sb.append(")");

      if (partitionKey != null && !partitionKey.isEmpty()) {
        sb.append(String.format(" PARTITION BY HASH(%s) PARTITIONS 32", partitionKey));
      }
      return sb.toString();
    } catch (IntrospectionException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getCreateSingleColumnTableSql(
      String idColumnName, String stringColumnName, String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("CREATE TABLE IF NOT EXISTS %s (", tableName));
    sb.append("id BIGINT NOT NULL AUTO_INCREMENT, ");
    sb.append(stringColumnName);
    sb.append(" ");
    sb.append("TEXT");
    sb.append(String.format(", PRIMARY KEY (%s)", idColumnName));
    sb.append(")");
    return sb.toString();
  }

  public static String getJdbcTypeString(
      PropertyDescriptor beanProperty,
      boolean isPrimaryKeyOrUniqueKey,
      boolean isDatetime,
      boolean isText) {
    int maxVarcharLength = isPrimaryKeyOrUniqueKey ? 150 : 250;
    String sqlTypeForString = isText ? "TEXT" : String.format("VARCHAR(%s)", maxVarcharLength);
    if (isDatetime || beanProperty.getPropertyType().equals(Date.class)) {
      return "DATETIME";
    } else if (beanProperty.getPropertyType().equals(String.class)) {
      return sqlTypeForString;
    } else if (beanProperty.getPropertyType().equals(Integer.class)) {
      return "INT";
    } else if (beanProperty.getPropertyType().equals(Long.class)) {
      return "BIGINT";
    } else if (beanProperty.getPropertyType().equals(Float.class)) {
      return "FLOAT";
    } else if (beanProperty.getPropertyType().equals(Double.class)) {
      return "DOUBLE";
    } else if (beanProperty.getPropertyType().equals(Boolean.class)) {
      return "TINYINT";
    } else {
      throw new RuntimeException(
          String.format(
              "Unsupported property type for JDBC: %s, %s",
              beanProperty.getName(), beanProperty.getPropertyType()));
    }
  }

  public static java.sql.Timestamp getSqlTimestamp(Object obj) {
    if (obj == null) {
      return null;
    }

    final long millis;

    if (obj instanceof Date) {
      millis = ((Date) obj).getTime();
    } else if (obj instanceof java.sql.Timestamp) {
      millis = ((java.sql.Timestamp) obj).getTime();
    } else if (obj instanceof Double) {
      millis = DateTimeUtils.getMillisSmart(((Double) obj).doubleValue());
    } else if (obj instanceof Float) {
      millis = DateTimeUtils.getMillisSmart(((Float) obj).doubleValue());
    } else if (obj instanceof Long) {
      millis = DateTimeUtils.getMillisSmart(((Long) obj).longValue());
    } else if (obj instanceof Integer) {
      millis = DateTimeUtils.getMillisSmart(((Integer) obj).longValue());
    } else {
      throw new RuntimeException(
          String.format("Cannot get sql timestamp from %s (%s)", obj, obj.getClass()));
    }

    return new java.sql.Timestamp(millis);
  }
}
