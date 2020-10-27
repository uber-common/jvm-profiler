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

package com.uber.profiling.reporters;

import java.util.Map;

public class CpuAndMemoryProfilerMetric {
  private Long epochMillis;
  private String name;
  private String host;
  private String processUuid;
  private String appId;

  private String tag;
  private String role;
  private Double processCpuLoad;
  private Double systemCpuLoad;
  private Long processCpuTime;
  private Long heapMemoryTotalUsed;
  private Long heapMemoryCommitted;
  private Long heapMemoryMax;
  private Long nonHeapMemoryTotalUsed;
  private Long nonHeapMemoryCommitted;
  private Long nonHeapMemoryMax;
  private Long vmRSS;
  private Long vmHWM;
  private Long vmSize;
  private Long vmPeak;

  public Long getEpochMillis() {
    return epochMillis;
  }

  public void setEpochMillis(Long epochMillis) {
    this.epochMillis = epochMillis;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getProcessUuid() {
    return processUuid;
  }

  public void setProcessUuid(String processUuid) {
    this.processUuid = processUuid;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public Double getProcessCpuLoad() {
    return processCpuLoad;
  }

  public void setProcessCpuLoad(Double processCpuLoad) {
    this.processCpuLoad = processCpuLoad;
  }

  public Double getSystemCpuLoad() {
    return systemCpuLoad;
  }

  public void setSystemCpuLoad(Double systemCpuLoad) {
    this.systemCpuLoad = systemCpuLoad;
  }

  public Long getProcessCpuTime() {
    return processCpuTime;
  }

  public void setProcessCpuTime(Long processCpuTime) {
    this.processCpuTime = processCpuTime;
  }

  public Long getHeapMemoryTotalUsed() {
    return heapMemoryTotalUsed;
  }

  public void setHeapMemoryTotalUsed(Long heapMemoryTotalUsed) {
    this.heapMemoryTotalUsed = heapMemoryTotalUsed;
  }

  public Long getHeapMemoryCommitted() {
    return heapMemoryCommitted;
  }

  public void setHeapMemoryCommitted(Long heapMemoryCommitted) {
    this.heapMemoryCommitted = heapMemoryCommitted;
  }

  public Long getHeapMemoryMax() {
    return heapMemoryMax;
  }

  public void setHeapMemoryMax(Long heapMemoryMax) {
    this.heapMemoryMax = heapMemoryMax;
  }

  public Long getNonHeapMemoryTotalUsed() {
    return nonHeapMemoryTotalUsed;
  }

  public void setNonHeapMemoryTotalUsed(Long nonHeapMemoryTotalUsed) {
    this.nonHeapMemoryTotalUsed = nonHeapMemoryTotalUsed;
  }

  public Long getNonHeapMemoryCommitted() {
    return nonHeapMemoryCommitted;
  }

  public void setNonHeapMemoryCommitted(Long nonHeapMemoryCommitted) {
    this.nonHeapMemoryCommitted = nonHeapMemoryCommitted;
  }

  public Long getNonHeapMemoryMax() {
    return nonHeapMemoryMax;
  }

  public void setNonHeapMemoryMax(Long nonHeapMemoryMax) {
    this.nonHeapMemoryMax = nonHeapMemoryMax;
  }

  public Long getVmRSS() {
    return vmRSS;
  }

  public void setVmRSS(Long vmRSS) {
    this.vmRSS = vmRSS;
  }

  public Long getVmHWM() {
    return vmHWM;
  }

  public void setVmHWM(Long vmHWM) {
    this.vmHWM = vmHWM;
  }

  public Long getVmSize() {
    return vmSize;
  }

  public void setVmSize(Long vmSize) {
    this.vmSize = vmSize;
  }

  public Long getVmPeak() {
    return vmPeak;
  }

  public void setVmPeak(Long vmPeak) {
    this.vmPeak = vmPeak;
  }
}
