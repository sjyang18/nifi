/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.reporting.azure.loganalytics;

import static org.junit.Assert.assertNotNull;
import java.util.List;
import org.apache.nifi.reporting.azure.loganalytics.MetricNames;

public class TestVerification {

    static public void assertDatatFlowMetrics(List<Metric> collectedMetrics) {
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.FLOW_FILES_RECEIVED) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.BYTES_RECEIVED) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.FLOW_FILES_SENT) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.BYTES_SENT) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.FLOW_FILES_QUEUED) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.BYTES_QUEUED) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.BYTES_READ) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.BYTES_WRITTEN) && o.CategoryName.equals("DataFlow")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.ACTIVE_THREADS) && o.CategoryName.equals("DataFlow")));
    }

    static public void assertJVMMetrics(List<Metric> collectedMetrics) {
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_HEAP_USED) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_GC_TIME) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_THREAD_COUNT) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_FILE_DESCRIPTOR_USAGE) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_DAEMON_THREAD_COUNT) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_THREAD_STATES_BLOCKED) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_UPTIME) && o.CategoryName.equals("JvmMetrics")));
        assertNotNull( collectedMetrics.stream().anyMatch(
            o -> o.Name.equals(MetricNames.JVM_HEAP_USAGE) && o.CategoryName.equals("JvmMetrics")));
    }
}