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

package com.uber.profiling.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class YamlConfigProviderTest {
    @Test
    public void getConfig() throws IOException {
        {
            YamlConfigProvider provider = new YamlConfigProvider(null);
            Assert.assertEquals(0, provider.getConfig().size());
        }

        {
            YamlConfigProvider provider = new YamlConfigProvider("");
            Assert.assertEquals(0, provider.getConfig().size());
        }

        {
            YamlConfigProvider provider = new YamlConfigProvider("not_exiting_file");
            Assert.assertEquals(0, provider.getConfig().size());
        }
        
        {
            File file = File.createTempFile("test", "test");
            file.deleteOnExit();

            String content = "";
            Files.write(file.toPath(), content.getBytes(), StandardOpenOption.CREATE);

            YamlConfigProvider provider = new YamlConfigProvider(file.getAbsolutePath());
            Assert.assertEquals(0, provider.getConfig().size());
        }

        {
            File file = File.createTempFile("test", "test");
            file.deleteOnExit();

            String content = "key1: value1\n" +
                    "key2:\n" +
                    "- value2a\n" +
                    "- value2b\n" +
                    "override:\n" +
                    "  override1: \n" +
                    "    key1: value11\n" +
                    "    key2:\n" +
                    "    - value22a\n" +
                    "    - value22b\n" +
                    "";
            Files.write(file.toPath(), content.getBytes(), StandardOpenOption.CREATE);

            YamlConfigProvider provider = new YamlConfigProvider(file.getAbsolutePath());
            Map<String, Map<String, List<String>>> config = provider.getConfig();
            Assert.assertEquals(2, config.size());

            Map<String, List<String>> rootConfig = config.get("");
            Assert.assertEquals(2, rootConfig.size());
            Assert.assertEquals(Arrays.asList("value1"), rootConfig.get("key1"));
            Assert.assertEquals(Arrays.asList("value2a", "value2b"), rootConfig.get("key2"));

            Map<String, List<String>> override1Config = config.get("override1");
            Assert.assertEquals(2, override1Config.size());
            Assert.assertEquals(Arrays.asList("value11"), override1Config.get("key1"));
            Assert.assertEquals(Arrays.asList("value22a", "value22b"), override1Config.get("key2"));
        }
    }
}
