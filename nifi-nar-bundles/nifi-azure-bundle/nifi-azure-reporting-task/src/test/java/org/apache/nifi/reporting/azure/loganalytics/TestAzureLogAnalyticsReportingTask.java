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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockReportingContext;
import org.apache.nifi.util.MockReportingInitializationContext;
import org.apache.nifi.util.MockVariableRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

public class TestAzureLogAnalyticsReportingTask {

    private static final String TEST_INIT_CONTEXT_ID = "test-init-context-id";
    private static final String TEST_INIT_CONTEXT_NAME = "test-init-context-name";
    private static final String TEST_TASK_ID = "test-azureloganalyticsreportingtask-id";
    private static final String MOCK_KEY = "abcdefg";
    private static final String TEST_GROUP1_ID= "testgpid1";
    private static final String TEST_GROUP2_ID= "testgpid2";
    private MockReportingInitializationContext reportingInitContextStub;
    private MockReportingContext reportingContextStub;
    private MockConfigurationContext configurationContextStub;
    private TestableAzureLogAnalyticsReportingTask testedReportingTask;
    private ProcessGroupStatus rootGroupStatus;
    private ProcessGroupStatus testGroupStatus;
    private ProcessGroupStatus testGroupStatus2;
    private ProcessorStatus procStatus;

    @Before
    public void setup() {
        testedReportingTask = new TestableAzureLogAnalyticsReportingTask();
        rootGroupStatus = new ProcessGroupStatus();
        reportingInitContextStub = new MockReportingInitializationContext(TEST_INIT_CONTEXT_ID, TEST_INIT_CONTEXT_NAME,
                new MockComponentLog(TEST_TASK_ID, testedReportingTask));

        reportingContextStub = new MockReportingContext(Collections.emptyMap(),
                new MockStateManager(testedReportingTask), new MockVariableRegistry());

        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.INSTANCE_ID.getName(), TEST_TASK_ID);
        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.LOG_ANALYTICS_WORKSPACE_ID.getName(), TEST_TASK_ID);
        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.LINUX_PRIMARY_KEY.getName(), MOCK_KEY);

        configurationContextStub = new MockConfigurationContext(reportingContextStub.getProperties(),
                reportingContextStub.getControllerServiceLookup());

        rootGroupStatus.setId("1234");
        rootGroupStatus.setFlowFilesReceived(5);
        rootGroupStatus.setBytesReceived(10000);
        rootGroupStatus.setFlowFilesSent(10);
        rootGroupStatus.setBytesSent(20000);
        rootGroupStatus.setQueuedCount(100);
        rootGroupStatus.setQueuedContentSize(1024L);
        rootGroupStatus.setBytesRead(60000L);
        rootGroupStatus.setBytesWritten(80000L);
        rootGroupStatus.setActiveThreadCount(5);
        rootGroupStatus.setName("root");
        rootGroupStatus.setFlowFilesTransferred(5);
        rootGroupStatus.setBytesTransferred(10000);
        rootGroupStatus.setOutputContentSize(1000L);
        rootGroupStatus.setInputContentSize(1000L);
        rootGroupStatus.setOutputCount(100);
        rootGroupStatus.setInputCount(1000);
        initProcessorStatuses();

    }

    private void initProcessorStatuses() {
        procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(123456789);
        procStatus.setInputCount(2);
        procStatus.setOutputCount(4);
        procStatus.setActiveThreadCount(6);
        procStatus.setBytesSent(1256);
        procStatus.setName("sampleProcessor");
        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        rootGroupStatus.setProcessorStatus(processorStatuses);

        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        rootGroupStatus.setProcessGroupStatus(groupStatuses);
    }

    private void initTestGroupStatuses() {
        testGroupStatus = new ProcessGroupStatus();
        testGroupStatus.setId(TEST_GROUP1_ID);
        testGroupStatus.setFlowFilesReceived(5);
        testGroupStatus.setBytesReceived(10000);
        testGroupStatus.setFlowFilesSent(10);
        testGroupStatus.setBytesSent(20000);
        testGroupStatus.setQueuedCount(100);
        testGroupStatus.setQueuedContentSize(1024L);
        testGroupStatus.setBytesRead(60000L);
        testGroupStatus.setBytesWritten(80000L);
        testGroupStatus.setActiveThreadCount(5);
        testGroupStatus.setName(TEST_GROUP1_ID);
        testGroupStatus.setFlowFilesTransferred(5);
        testGroupStatus.setBytesTransferred(10000);
        testGroupStatus.setOutputContentSize(1000L);
        testGroupStatus.setInputContentSize(1000L);
        testGroupStatus.setOutputCount(100);
        testGroupStatus.setInputCount(1000);
    }
    private void initTestGroup2Statuses() {
        testGroupStatus2 = new ProcessGroupStatus();
        testGroupStatus2.setId(TEST_GROUP2_ID);
        testGroupStatus2.setFlowFilesReceived(5);
        testGroupStatus2.setBytesReceived(10000);
        testGroupStatus2.setFlowFilesSent(10);
        testGroupStatus2.setBytesSent(20000);
        testGroupStatus2.setQueuedCount(100);
        testGroupStatus2.setQueuedContentSize(1024L);
        testGroupStatus2.setBytesRead(60000L);
        testGroupStatus2.setBytesWritten(80000L);
        testGroupStatus2.setActiveThreadCount(5);
        testGroupStatus2.setName(TEST_GROUP2_ID);
        testGroupStatus2.setFlowFilesTransferred(5);
        testGroupStatus2.setBytesTransferred(10000);
        testGroupStatus2.setOutputContentSize(1000L);
        testGroupStatus2.setInputContentSize(1000L);
        testGroupStatus2.setOutputCount(100);
        testGroupStatus2.setInputCount(1000);
    }
    @Test
    public void testOnTrigger() throws IOException, InterruptedException, InitializationException {
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        testedReportingTask.onTrigger(reportingContextStub);

        List<Metric> collectedMetrics = testedReportingTask.getMetricsCollected();
        TestVerification.assertDatatFlowMetrics(collectedMetrics);
    }
    @Test
    public void testOnTriggerWithOnePG() throws IOException, InterruptedException, InitializationException {
        initTestGroupStatuses();
        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.PROCESS_GROUP_IDS.getName(), TEST_GROUP1_ID);
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        reportingContextStub.getEventAccess().setProcessGroupStatus(TEST_GROUP1_ID, testGroupStatus);
        testedReportingTask.onTrigger(reportingContextStub);

        List<Metric> collectedMetrics = testedReportingTask.getMetricsCollected();
        TestVerification.assertDatatFlowMetrics(collectedMetrics);
    }
    @Test
    public void testOnTriggerWithPGList() throws IOException, InterruptedException, InitializationException {
        initTestGroupStatuses();
        initTestGroup2Statuses();
        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.PROCESS_GROUP_IDS.getName(),
            String.format("%s, %s", TEST_GROUP1_ID, TEST_GROUP2_ID));
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        reportingContextStub.getEventAccess().setProcessGroupStatus(TEST_GROUP1_ID, testGroupStatus);
        reportingContextStub.getEventAccess().setProcessGroupStatus(TEST_GROUP2_ID, testGroupStatus2);
        testedReportingTask.onTrigger(reportingContextStub);

        List<Metric> collectedMetrics = testedReportingTask.getMetricsCollected();
        TestVerification.assertDatatFlowMetrics(collectedMetrics);
    }

    @Test
    public void testEmitJVMMetrics() throws IOException, InterruptedException, InitializationException {
        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.SEND_JVM_METRICS.getName(), "true");
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        testedReportingTask.onTrigger(reportingContextStub);

        List<Metric> collectedMetrics = testedReportingTask.getMetricsCollected();
        TestVerification.assertJVMMetrics(collectedMetrics);
    }

    @Test
    public void testAuthorization() throws IOException, InterruptedException, InitializationException {

        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.SEND_JVM_METRICS.getName(), "true");
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        testedReportingTask.onTrigger(reportingContextStub);

        HttpsURLConnection mockConnection = testedReportingTask.getTestedMockConnection();
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection, atLeast(1)).setRequestProperty( eq("Authorization"), captor.capture());
        assertTrue(captor.getValue().contains("SharedKey"));
    }

    @Test
    public void testOutputStreamToLogAnalytics() throws IOException, InterruptedException, InitializationException {

        reportingContextStub.setProperty(AzureLogAnalyticsReportingTask.SEND_JVM_METRICS.getName(), "true");
        testedReportingTask.initialize(reportingInitContextStub);
        testedReportingTask.setup(configurationContextStub);
        reportingContextStub.getEventAccess().setProcessGroupStatus(rootGroupStatus);
        testedReportingTask.onTrigger(reportingContextStub);

        HttpsURLConnection mockConnection = testedReportingTask.getTestedMockConnection();
        verify(mockConnection, atLeast(1)).getOutputStream();
    }

    private class TestableAzureLogAnalyticsReportingTask extends AzureLogAnalyticsReportingTask {

        private List<Metric> metricsCollected;
        @Override
        protected void sendMetrics(final String workspaceId, final String linuxPrimaryKey, final String logName,
            final List<Metric> allMetrics) throws IOException{

            metricsCollected = allMetrics;
            super.sendMetrics(workspaceId, linuxPrimaryKey, logName, allMetrics);
        }

        public List<Metric> getMetricsCollected() {
            return metricsCollected;
        }

        private HttpsURLConnection mockConnection;

        @Override
        protected HttpsURLConnection getHttpsURLConnection(final String workspaceId, final String linuxPrimaryKey,
            final String logName) throws IOException{
            mockConnection = Mockito.mock(HttpsURLConnection.class);
            return  mockConnection;
        }
        protected HttpsURLConnection getTestedMockConnection(){
            return mockConnection;
        }
    }
}