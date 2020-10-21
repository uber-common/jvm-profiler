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
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class BeanUtils {
  public static <T> T merge(T oldBean, T newBean) {
    T result = null;
    try {
      result = (T) oldBean.getClass().getConstructor().newInstance();

      BeanInfo beanInfo = Introspector.getBeanInfo(oldBean.getClass());

      for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {

        // Only copy writable attributes
        if (descriptor.getWriteMethod() != null) {
          Object value1 = descriptor.getReadMethod().invoke(oldBean);
          Object value2 = descriptor.getReadMethod().invoke(newBean);

          if (value1 == null && value2 != null) {
            descriptor.getWriteMethod().invoke(result, value2);
          } else if (value1 != null && value2 == null) {
            descriptor.getWriteMethod().invoke(result, value1);
          } else if (value1 != null && value2 != null) {
            descriptor.getWriteMethod().invoke(result, value2);
          }
        }
      }

      return result;
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | IntrospectionException e) {
      throw new RuntimeException(
          String.format("Failed to merge two beans, old: %s, new: %s", oldBean, newBean), e);
    }
  }

  public static Map<String, PropertyDescriptor> getPropertyDescriptorMapWithLowerCaseName(
      Class<?> clazz) {
    try {
      Map<String, PropertyDescriptor> result = new HashMap<>();

      BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
      for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
        if (descriptor.getWriteMethod() == null || descriptor.getReadMethod() == null) {
          continue;
        }
        result.put(descriptor.getName().toLowerCase(), descriptor);
      }

      return result;
    } catch (IntrospectionException e) {
      throw new RuntimeException(
          String.format("Failed to get properties for bean class: %s", clazz), e);
    }
  }
}
