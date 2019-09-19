package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;
import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metrics reporter class for Graphite.
 *
 * Check the "host" and "port" properties for Graphite and update accordingly.
 *
 * You can also pass Graphite connection properties from yaml file and those
 * properties will be used by this reporter.
 *
 * To uses GraphiteOutputReporter with properties pass it in command.
 *
 *     reporter=com.uber.profiling.reporters.GraphiteOutputReporter
 *
 * To use properties from yaml file use below command.
 *
 *     reporter=com.uber.profiling.reporters.GraphiteOutputReporter,configProvider=com.uber.profiling.YamlConfigProvider,configFile=/opt/graphite.yaml
 *
 */
public class GraphiteOutputReporter implements Reporter {

  private static final AgentLogger logger = AgentLogger
      .getLogger(GraphiteOutputReporter.class.getName());
  private String host = "127.0.0.1";
  private int port = 2003;
  private String prefix = "jvm";
  private Socket socket = null;
  private PrintWriter out = null;

  @Override
  public void report(String profilerName, Map<String, Object> metrics) {
    // get DB connection
    ensureGraphiteConnection();
    // format metrics
    logger.info("Profiler Name : " + profilerName);
    String tag = ((String) metrics.computeIfAbsent("tag", v -> "default_tag"))
        .replaceAll("\\.", "-");
    String appId = ((String) metrics.computeIfAbsent("appId", v -> "default_app"))
        .replaceAll("\\.", "-");
    String host = ((String) metrics.computeIfAbsent("host", v -> "unknown_host"))
        .replaceAll("\\.", "-");
    String process = ((String) metrics.computeIfAbsent("processUuid", v -> "unknown_process"))
        .replaceAll("\\.", "-");
    String newPrefix = String.join(".", prefix, tag, appId, host, process);

    Map<String, Object> formattedMetrics = getFormattedMetrics(metrics);
    long timestamp = System.currentTimeMillis() / 1000;
    for (Map.Entry<String, Object> entry : formattedMetrics.entrySet()) {
      out.printf(
          newPrefix + "." + entry.getKey() + " " + entry.getValue() + " " + timestamp + "%n");
    }
  }

  // Format metrics in key=value (line protocol)
  private Map<String, Object> getFormattedMetrics(Map<String, Object> metrics) {
    Map<String, Object> formattedMetrics = new HashMap<>();
    for (Map.Entry<String, Object> entry : metrics.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      logger.debug("Raw Metric-Name = " + key + ", Metric-Value = " + value);
      if (value != null && value instanceof List) {
        List listValue = (List) value;
        if (!listValue.isEmpty() && listValue.get(0) instanceof String) {
          List<String> metricList = (List<String>) listValue;
          formattedMetrics.put(key, String.join(",", metricList));
        } else if (!listValue.isEmpty() && listValue.get(0) instanceof Map) {
          List<Map<String, Object>> metricList = (List<Map<String, Object>>) listValue;
          int num = 1;
          for (Map<String, Object> metricMap : metricList) {
            String name = null;
            if (metricMap.containsKey("name") && metricMap.get("name") != null && metricMap
                .get("name") instanceof String) {
              name = (String) metricMap.get("name");
              name = name.replaceAll("\\s", "");
            }
            for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
              if (StringUtils.isNotEmpty(name)) {
                formattedMetrics.put(key + "." + name + "." + entry1.getKey(), entry1.getValue());
              } else {
                formattedMetrics.put(key + "." + entry1.getKey() + "." + num, entry1.getValue());
              }
            }
            num++;
          }
        }
      } else if (value != null && value instanceof Map) {
        Map<String, Object> metricMap = (Map<String, Object>) value;
        for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
          String key1 = entry1.getKey();
          Object value1 = entry1.getValue();
          if (value1 != null && value1 instanceof Map) {
            Map<String, Object> value2 = (Map<String, Object>) value1;
            int num = 1;
            for (Map.Entry<String, Object> entry2 : value2.entrySet()) {
              formattedMetrics
                  .put(key + "." + key1 + "." + entry2.getKey() + "." + num, entry2.getValue());
            }
            num++;
          }
        }
      } else {
        formattedMetrics.put(key, value);
      }
    }
    return formattedMetrics;
  }

  private void ensureGraphiteConnection() {
    if (socket == null) {
      synchronized (this) {
        if (socket == null) {
          try {
            logger.info("connecting to graphite(" + host + ":" + port + ")!");
            socket = new Socket(host, port);
            OutputStream s = socket.getOutputStream();
            out = new PrintWriter(s, true);
          } catch (IOException e) {
            logger.warn("connect to graphite error!", e);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    try {
      if (out != null) {
        out.close();
      }
      if (socket != null) {
        socket.close();
      }
    } catch (IOException e) {
      logger.warn("close connection to graphite error!", e);
    }
  }

  // properties from yaml file
  @Override
  public void updateArguments(Map<String, List<String>> connectionProperties) {
    for (Map.Entry<String, List<String>> entry : connectionProperties.entrySet()) {
      String key = entry.getKey();
      List<String> value = entry.getValue();
      if (StringUtils.isNotEmpty(key) && value != null && !value.isEmpty()) {
        String stringValue = value.get(0);
        if (key.equals("graphite.host")) {
          logger.info("Got value for host = " + stringValue);
          this.host = stringValue;
        } else if (key.equals("graphite.port")) {
          logger.info("Got value for port = " + stringValue);
          this.port = Integer.parseInt(stringValue);
        } else if (key.equals("graphite.prefix")) {
          logger.info("Got value for database = " + stringValue);
          this.prefix = stringValue;
        }
      }
    }
  }
}
