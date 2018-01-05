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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class AgentITCase {
    @Test
    public void runAgent() throws InterruptedException, IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin/java").toAbsolutePath().toString();

        String agentJar = getAgentJarPath();
        
        String outputDir = Files.createTempDirectory("ujagent_test_output").toString();
        System.out.println("outputDir: " + outputDir);
        
        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp",
                agentJar,
                "-javaagent:" + agentJar + "=reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=" + outputDir + ",tag=mytag,metricInterval=200,durationProfiling=com.uber.profiling.examples.HelloWorldApplication.publicSleepMethod,argumentProfiling=com.uber.profiling.examples.HelloWorldApplication.publicSleepMethod.1,ioProfiling=true",
                "com.uber.profiling.examples.HelloWorldApplication",
                "2000"
        );
        
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process process = pb.start();
        process.waitFor();

        File[] files = new File(outputDir).listFiles();
        Assert.assertEquals(5, files.length);

        List<String> fileNames = Arrays.asList(files).stream().map(t->t.getName()).sorted().collect(Collectors.toList());
        
        Assert.assertEquals("CpuAndMemory.json", fileNames.get(0));
        String jsonCpuAndMemory = new String(Files.readAllBytes(Paths.get(outputDir, fileNames.get(0))));
        System.out.println("-----CpuAndMemory-----");
        System.out.println(jsonCpuAndMemory);
        Assert.assertTrue(jsonCpuAndMemory.contains("bufferPool"));

        Assert.assertEquals("IO.json", fileNames.get(1));
        String jsonProcFileSystem = new String(Files.readAllBytes(Paths.get(outputDir, fileNames.get(1))));
        System.out.println("-----IO-----");
        System.out.println(jsonProcFileSystem);
        Assert.assertTrue(jsonProcFileSystem.contains("read_bytes"));
        Assert.assertTrue(jsonProcFileSystem.contains("write_bytes"));
        
        Assert.assertEquals("MethodArgument.json", fileNames.get(2));
        String jsonMethodArgument = new String(Files.readAllBytes(Paths.get(outputDir, fileNames.get(2))));
        System.out.println("-----MethodArgument-----");
        System.out.println(jsonMethodArgument);
        Assert.assertTrue(jsonMethodArgument.contains("arg.1"));
        
        Assert.assertEquals("MethodDuration.json", fileNames.get(3));
        String jsonMethodDuration = new String(Files.readAllBytes(Paths.get(outputDir, fileNames.get(3))));
        System.out.println("-----MethodDuration-----");
        System.out.println(jsonMethodDuration);
        Assert.assertTrue(jsonMethodDuration.contains("duration.sum"));

        Assert.assertEquals("ProcessInfo.json", fileNames.get(4));
        String jsonProcessInfo = new String(Files.readAllBytes(Paths.get(outputDir, fileNames.get(4))));
        System.out.println("-----ProcessInfo-----");
        System.out.println(jsonProcessInfo);
        Assert.assertTrue(jsonProcessInfo.contains("jvmClassPath"));
        Assert.assertTrue(jsonProcessInfo.contains(agentJar));
    }
    
    private String getAgentJarPath() throws IOException {
        // Find jar file with largest size under target directory, which should be the packaged agent jar file
        String agentJar = Files.list(Paths.get("target"))
                .max(Comparator.comparingLong(t -> {
                    try {
                        return Files.size(t);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .map(t->t.toString())
                .filter(t->t.endsWith(".jar"))
                .get();
        System.out.println("agentJar: " + agentJar);
        return agentJar;
    }
}
