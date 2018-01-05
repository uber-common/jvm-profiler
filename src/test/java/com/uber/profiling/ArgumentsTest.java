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

import com.uber.profiling.reporters.ConsoleOutputReporter;
import com.uber.profiling.util.ClassAndMethod;
import com.uber.profiling.util.ClassMethodArgument;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ArgumentsTest {
    @Test
    public void allArguments() {
        Arguments arguments = Arguments.parseArgs("reporter=com.uber.profiling.ArgumentsTest$DummyReporter,durationProfiling=a.bc.foo,metricInterval=123,appIdRegex=app123,argumentProfiling=package1.class1.method1.1");
        Assert.assertEquals(5, arguments.getRawArgValues().size());
        Assert.assertEquals(DummyReporter.class, arguments.getReporter().getClass());
        Assert.assertEquals(1, arguments.getDurationProfiling().size());
        Assert.assertEquals(new ClassAndMethod("a.bc", "foo"), arguments.getDurationProfiling().get(0));
        Assert.assertEquals(123, arguments.getMetricInterval());
        Assert.assertEquals("app123", arguments.getAppIdRegex());

        Assert.assertEquals(1, arguments.getArgumentProfiling().size());
        Assert.assertEquals(new ClassMethodArgument("package1.class1", "method1", 1), arguments.getArgumentProfiling().get(0));
    }

    @Test
    public void emptyArguments() {
        Arguments arguments = Arguments.parseArgs("");
        Assert.assertEquals(0, arguments.getRawArgValues().size());
        Assert.assertEquals(ConsoleOutputReporter.class, arguments.getReporter().getClass());
        Assert.assertEquals(0, arguments.getDurationProfiling().size());
        Assert.assertEquals(60000, arguments.getMetricInterval());
        Assert.assertEquals(0, arguments.getArgumentProfiling().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyArgumentValue() {
        Arguments.parseArgs("reporter=,durationProfiling=,metricInterval=,appIdRegex=,");
    }

    @Test
    public void durationProfiling() {
        Arguments arguments = Arguments.parseArgs("durationProfiling=a.bc.foo,durationProfiling=ab.c.d.test");
        Assert.assertEquals(2, arguments.getDurationProfiling().size());
        Assert.assertEquals(new ClassAndMethod("a.bc", "foo"), arguments.getDurationProfiling().get(0));
        Assert.assertEquals(new ClassAndMethod("ab.c.d", "test"), arguments.getDurationProfiling().get(1));
        Assert.assertEquals(Arguments.DEFAULT_METRIC_INTERVAL, arguments.getMetricInterval());
        Assert.assertEquals(Arguments.DEFAULT_APP_ID_REGEX, arguments.getAppIdRegex());
    }

    @Test
    public void argumentProfiling() {
        Arguments arguments = Arguments.parseArgs("durationProfiling=a.bc.foo,durationProfiling=ab.c.d.test");
        Assert.assertEquals(2, arguments.getDurationProfiling().size());
        Assert.assertEquals(new ClassAndMethod("a.bc", "foo"), arguments.getDurationProfiling().get(0));
        Assert.assertEquals(new ClassAndMethod("ab.c.d", "test"), arguments.getDurationProfiling().get(1));
        Assert.assertEquals(Arguments.DEFAULT_METRIC_INTERVAL, arguments.getMetricInterval());
        Assert.assertEquals(Arguments.DEFAULT_APP_ID_REGEX, arguments.getAppIdRegex());
    }

    @Test
    public void setReporter() {
        Arguments arguments = Arguments.parseArgs("");

        arguments.setReporter("com.uber.profiling.ArgumentsTest$DummyReporter");
        Reporter reporter = arguments.getReporter();
        Assert.assertTrue(reporter instanceof com.uber.profiling.ArgumentsTest.DummyReporter);
    }

    public static class DummyReporter implements Reporter {
        @Override
        public void report(String profilerName, Map<String, Object> metrics) {
        }

        @Override
        public void close() {
        }
    }
}
