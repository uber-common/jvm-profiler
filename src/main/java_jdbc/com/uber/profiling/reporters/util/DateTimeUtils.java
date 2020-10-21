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
import org.apache.commons.lang3.math.NumberUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTimeUtils {
  private static final AgentLogger logger = AgentLogger.getLogger(DateTimeUtils.class.getName());

  public static Calendar getUtcCalendar() {
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    Calendar calendar = Calendar.getInstance(timeZone);
    return calendar;
  }

  public static String formatAsIso(long millis) {
    return formatAsIso(new Date(millis));
  }

  public static String formatAsIso(Date date) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'");
    df.setTimeZone(tz);
    return df.format(date);
  }

  public static String formatAsIsoWithoutMillis(long millis) {
    return formatAsIsoWithoutMillis(new Date(millis));
  }

  public static String formatAsIsoWithoutMillis(Date date) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    df.setTimeZone(tz);
    return df.format(date);
  }

  public static Date parseDateTimeSmart(String str) {
    if (NumberUtils.isCreatable(str)) {
      double doubleValue = Double.parseDouble(str);
      Date dt = new Date((long) doubleValue);
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      cal.setTime(dt);
      int year = cal.get(Calendar.YEAR);
      if (year >= 2000 && year < 3000) {
        return dt;
      }
      return new Date((long) doubleValue * 1000);
    } else {
      return parseIsoDateTime(str);
    }
  }

  public static long getMillisSmart(double value) {
    Date dt = new Date((long) value);
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTime(dt);
    int year = cal.get(Calendar.YEAR);
    if (year >= 2000 && year < 3000) {
      return (long) value;
    }
    return (long) (value * 1000);
  }

  public static long getMillisSmart(long value) {
    Date dt = new Date(value);
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTime(dt);
    int year = cal.get(Calendar.YEAR);
    if (year >= 2000 && year < 3000) {
      return value;
    }
    return value * 1000;
  }

  public static Date parseIsoDateTime(String str) {
    try {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
      LocalDateTime localDateTime = LocalDateTime.parse(str, dtf);
      return Date.from(localDateTime.atOffset(ZoneOffset.UTC).toInstant());
    } catch (IllegalArgumentException e) {
      logger.debug(String.format("Failed to parse date time %s with exception %s, will try another format", str, e));
    }

    try {
      DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
      LocalDateTime localDateTime = LocalDateTime.parse(str, dtf);
      return Date.from(localDateTime.atOffset(ZoneOffset.UTC).toInstant());
    } catch (IllegalArgumentException e) {
      logger.debug(String.format("Failed to parse date time %s with exception %s, will try another format", str, e));
    }

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    TimeZone tz = TimeZone.getTimeZone("UTC");
    df.setTimeZone(tz);
    try {
      return df.parse(str);
    } catch (ParseException e) {
      logger.debug(String.format("Failed to parse date time %s with exception %s, will try another format", str, e));
    }

    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    df.setTimeZone(tz);
    try {
      return df.parse(str);
    } catch (ParseException e) {
      throw new RuntimeException("Failed to parse date time: " + str, e);
    }
  }

  public static Date addDays(Date date, int days) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, days);
    return c.getTime();
  }

  public static long truncateToDay(long millis) {
    long millisPerDay = 1000 * 60 * 60 * 24;
    return (millis / millisPerDay) * millisPerDay;
  }

  public static Date truncateToDay(Date date) {
    long millisPerDay = 1000 * 60 * 60 * 24;
    return new Date((date.getTime() / millisPerDay) * millisPerDay);
  }

  public static int getDayOfMonth(Date date) {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTime(date);
    int day = cal.get(Calendar.DAY_OF_MONTH);
    return day;
  }

}
