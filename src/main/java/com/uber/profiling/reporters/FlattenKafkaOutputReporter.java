package com.uber.profiling.reporters;

import com.uber.profiling.ArgumentUtils;
import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FlattenKafkaOutputReporter: a custom KafkaOutputReporter
 * This reporter will transform metrics as key=value output format before sending to kafka
 * It would be useful in case you want to report the metrics into kafka -> consume those metrics in somewhere & then writing to InfluxDB indirectly.
 * Use in case your JVM machine vs the influxdb instance are hosted in different cluster or different network.
 */
public class FlattenKafkaOutputReporter extends KafkaOutputReporter{
    public final static String ARG_TOPIC_LOWERCASE = "topicLowercase";

    private static final AgentLogger logger = AgentLogger.getLogger(FlattenKafkaOutputReporter.class.getName());

    private Boolean topicLowercase;
    public Boolean getTopicLowercase() {
        return topicLowercase;
    }
    public void setTopicLowercase(Boolean topicLowercase) {this.topicLowercase = topicLowercase;}

    public FlattenKafkaOutputReporter(String brokerList, boolean syncMode, String topicPrefix, Boolean topicLowercase) {
        super(brokerList, syncMode, topicPrefix);
        this.topicLowercase = topicLowercase;
    }

    @Override
    public void updateArguments(Map<String, List<String>> parsedArgs) {
        super.updateArguments(parsedArgs);
        String argValue = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_TOPIC_LOWERCASE);
        if (ArgumentUtils.needToUpdateArg(argValue)) {
            setTopicLowercase(Boolean.parseBoolean(argValue));
            logger.info("Got argument value for topicLowercase: " + topicLowercase);
        }
    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        Map<String, Object> formattedMetrics = getFormattedMetrics(metrics);
        super.report(profilerName, formattedMetrics);
    }

    @Override
    public String getTopic(String profilerName) {
        String topic = super.getTopic(profilerName);
        if (topicLowercase) {
            return topic.toLowerCase();
        }
        return topic;
    }

    // Format metrics in key=value (line protocol)
    private Map<String, Object> getFormattedMetrics(Map<String, Object> metrics) {
        Map<String, Object> formattedMetrics = new HashMap<>();
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                List listValue = (List) value;
                if (!listValue.isEmpty() && listValue.get(0) instanceof String) {
                    List<String> metricList = (List<String>) listValue;
                    formattedMetrics.put(key, String.join(",", metricList));
                } else if (!listValue.isEmpty() && listValue.get(0) instanceof Map) {
                    List<Map<String, Object>> metricList = (List<Map<String, Object>>) listValue;
                    int num = 1;
                    for (Map<String, Object> metricMap : metricList) {
                        String name = null;
                        if(metricMap.containsKey("name") && metricMap.get("name") != null && metricMap.get("name") instanceof String){
                            name = (String) metricMap.get("name");
                            name = name.replaceAll("\\s", "");
                        }
                        for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                            if(StringUtils.isNotEmpty(name)){
                                formattedMetrics.put(key + "-" + name + "-" + entry1.getKey(), entry1.getValue());
                            }else{
                                formattedMetrics.put(key + "-" + entry1.getKey() + "-" + num, entry1.getValue());
                            }
                        }
                        num++;
                    }
                }
            } else if (value instanceof Map) {
                Map<String, Object> metricMap = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                    String key1 = entry1.getKey();
                    Object value1 = entry1.getValue();
                    if (value1 instanceof Map) {
                        Map<String, Object> value2 = (Map<String, Object>) value1;
                        int num = 1;
                        for (Map.Entry<String, Object> entry2 : value2.entrySet()) {
                            formattedMetrics.put(key + "-" + key1 + "-" + entry2.getKey() + "-" + num, entry2.getValue());
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

}
