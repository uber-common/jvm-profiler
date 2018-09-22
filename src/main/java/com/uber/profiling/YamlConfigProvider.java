/*
 * Copyright (c) 2018 Uber Technologies, Inc.
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

package com.uber.profiling;

import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.ExponentialBackoffRetryPolicy;
import com.uber.profiling.util.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlConfigProvider implements ConfigProvider {
    private static final AgentLogger logger = AgentLogger.getLogger(YamlConfigProvider.class.getName());
    
    private static final String OVERRIDE_KEY = "override";
    
    private String filePath;

    public YamlConfigProvider() {
    }
    
    public YamlConfigProvider(String filePath) {
        setFilePath(filePath);
    }

    public void setFilePath(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath cannot be null or empty");
        }
        
        this.filePath = filePath;
    }

    @Override
    public Map<String, Map<String, List<String>>> getConfig() {
        return getConfig(filePath);
    }
    
    private static Map<String, Map<String, List<String>>> getConfig(String configFilePathOrUrl) {
        if (configFilePathOrUrl == null || configFilePathOrUrl.isEmpty()) {
            logger.warn("Empty YAML config file path");
            return new HashMap<>();
        }

        byte[] bytes;
        
        try {
            bytes = new ExponentialBackoffRetryPolicy<byte[]>(3, 100)
                    .attempt(() -> {
                        String filePathLowerCase = configFilePathOrUrl.toLowerCase();
                        if (filePathLowerCase.startsWith("http://") || filePathLowerCase.startsWith("https://")) {
                            return getHttp(configFilePathOrUrl);
                        } else {
                            return Files.readAllBytes(Paths.get(configFilePathOrUrl));
                        }
                    });
            
            logger.info("Read YAML config from: " + configFilePathOrUrl);
        } catch (Throwable e) {
            logger.warn("Failed to read file: " + configFilePathOrUrl, e);
            return new HashMap<>();
        }

        if (bytes == null || bytes.length == 0) {
            return new HashMap<>();
        }
        
        Map<String, Map<String, List<String>>> result = new HashMap<>();
        
        try {
            try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                Yaml yaml = new Yaml();
                Object yamlObj = yaml.load(stream);

                if (!(yamlObj instanceof Map)) {
                    logger.warn("Invalid YAML config content: " + yamlObj);
                    return result;
                }

                Map yamlMap = (Map)yamlObj;
                
                Map overrideMap = null;
                
                for (Object key : yamlMap.keySet()) {
                    String configKey = key.toString();
                    Object valueObj = yamlMap.get(key);
                    if (valueObj == null) {
                        continue;
                    }
                    
                    if (configKey.equals(OVERRIDE_KEY)) {
                        if (valueObj instanceof Map) {
                            overrideMap = (Map)valueObj;
                        } else {
                            logger.warn("Invalid override property: " + valueObj);
                        }
                    } else {
                        if (valueObj instanceof Map) {
                            addMapConfig(result, "", configKey, (Map)valueObj);
                        }else{
                            addConfig(result, "", configKey, valueObj);
                        }
                    }
                }
                
                if (overrideMap != null) {
                    for (Object key : overrideMap.keySet()) {
                        String overrideKey = key.toString();
                        Object valueObj = overrideMap.get(key);
                        if (valueObj == null) {
                            continue;
                        }

                        if (!(valueObj instanceof Map)) {
                            logger.warn("Invalid override section: " + key + ": " + valueObj);
                            continue;
                        }
                        
                        Map<Object, Object> overrideValues = (Map<Object, Object>)valueObj;
                        addMapConfig(result, overrideKey, OVERRIDE_KEY + "." + overrideKey, overrideValues);
                    }
                }

                return result;
            }
        } catch (Throwable e) {
            logger.warn("Failed to read config file: " + configFilePathOrUrl, e);
            return new HashMap<>();
        }
    }
    
    private static void addConfig(Map<String, Map<String, List<String>>> config, String override, String key, Object value) {
        Map<String, List<String>> configMap = config.computeIfAbsent(override, k -> new HashMap<>());
        List<String> configValueList = configMap.computeIfAbsent(key, k -> new ArrayList<>());
        
        if (value instanceof List) {
            for (Object entry : (List)value) {
                configValueList.add(entry.toString());
            }
        } else if (value instanceof Object[]) {
            for (Object entry : (Object[])value) {
                configValueList.add(entry.toString());
            }
        } else {
            configValueList.add(value.toString());
        }
    }
    
    private static void addMapConfig(Map<String, Map<String, List<String>>> result, String override, String configKey, Map valueObj) {
        for (Object key : valueObj.keySet()) {
            String propKey = key.toString();
            Object propValue = valueObj.get(propKey);
            if (propValue != null) {
                if (propValue instanceof Map) {
                    addMapConfig(result, override, configKey + "." + propKey, (Map) propValue);
                } else {
                    addConfig(result, override, configKey + "." + propKey, propValue);
                }
            }
        }
    }

    private static byte[] getHttp(String url) {
        try {
            logger.debug(String.format("Getting url: %s", url));
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpGet httpGet = new HttpGet(url);
                try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    if (statusCode != HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Failed response from url: " + url + ", response code: " + statusCode);
                    }
                    // TODO handle charset encoding
                    return IOUtils.toByteArray(httpResponse.getEntity().getContent());
                }
            }
        }
        catch (Throwable ex) {
            throw new RuntimeException("Failed getting url: " + url, ex);
        }
    }
}
