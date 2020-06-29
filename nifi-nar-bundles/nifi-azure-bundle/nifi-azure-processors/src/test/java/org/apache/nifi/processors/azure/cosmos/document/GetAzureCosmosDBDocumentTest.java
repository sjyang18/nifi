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
package org.apache.nifi.processors.azure.cosmos.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.azure.core.util.paging.ContinuablePagedFlux;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.CosmosPagedFluxOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import reactor.core.publisher.Flux;


public class GetAzureCosmosDBDocumentTest extends MockTestBase {

    private static final String MOCK_URI = "MOCK_URI";
    private static final String MOCK_DB_ACCESS_KEY = "MOCK_DB_ACCESS_KEY";
    private static final String MOCK_QUERY = "select * from c";

    private MockGetCosmosDocument processor;

    @Before
    public void setUp() throws Exception {
        processor = new MockGetCosmosDocument();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setIncomingConnection(false);
        testRunner.setNonLoopConnection(false);
    }

    @Test
    public void testProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.URI,MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBDocument.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBDocument.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
        // setup connnection controller service
    }

    @Test
    public void testReadDocuments() throws Exception {
        prepareMockProcess();
        testRunner.enqueue(new byte[] {});
        testRunner.run();
        final List<JsonNode> mockData = processor.getMockData();

        testRunner.assertAllFlowFilesTransferred(AbstractAzureCosmosDBProcessor.REL_SUCCESS, mockData.size());
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetAzureCosmosDBDocument.REL_SUCCESS);
        for (int idx = 0; idx < 10; idx++) {
            final MockFlowFile flowFile = flowFiles.get(idx);
            flowFile.assertContentEquals(mockData.get(idx).toString());
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    @Test
    public void testMultipleResultsInFlowFile() throws Exception {
        final JsonParser parser = new JsonParser();
        prepareMockProcess();
        testRunner.setProperty(GetAzureCosmosDBDocument.RESULTS_PER_FLOWFILE, "3");
        testRunner.enqueue(new byte[] {});
        testRunner.run();
        final List<JsonNode> mockData = processor.getMockData();
        final int expectedNumOfFlowFiles = (int) Math.ceil(mockData.size() / 3.0);
        // expectation: 4 flowfils with each having  [3,3,3,1] records
        testRunner.assertAllFlowFilesTransferred(GetAzureCosmosDBDocument.REL_SUCCESS, expectedNumOfFlowFiles);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetAzureCosmosDBDocument.REL_SUCCESS);
        for (int idx = 0; idx < expectedNumOfFlowFiles; idx++) {
            final MockFlowFile flowFile = flowFiles.get(idx);
            final JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
            final JsonArray jArray = parsedJson.getAsJsonArray();
            if (idx < expectedNumOfFlowFiles - 1) {
                assertEquals(3, jArray.size()); // all other than last flow file should have 3 Json records
            }
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    private void prepareMockProcess() throws Exception {
        setBasicMockProperties(true);
        testRunner.setProperty(GetAzureCosmosDBDocument.QUERY,MOCK_QUERY);
        testRunner.assertValid();
    }

}

@SuppressWarnings("unchecked")
class MockGetCosmosDocument extends GetAzureCosmosDBDocument {

    List<JsonNode> mockData;
    CosmosPagedIterable<JsonNode> mockPagedIterable;

    @Override
    protected void createCosmosClient(String uri, String accessKey, ConsistencyLevel clevel) throws NullPointerException, CosmosException {
        // create a mock DocumentClient
        this.cosmosClient = mock(CosmosClient.class);

    }

    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws ProcessException {
        this.container = mock(CosmosContainer.class);
        mockPagedIterable  = mockResponseData();
        when(this.container.queryItems(anyString(), any(), eq(JsonNode.class))).thenReturn(mockPagedIterable);
    }

    public List<JsonNode> getMockData() {
        return mockData;
    }

    private CosmosPagedIterable<JsonNode> mockResponseData() {

        final ObjectMapper mapper = new ObjectMapper();
        CosmosPagedIterable<JsonNode> cosmosPagedIterable = null;
        mockData = new ArrayList<JsonNode>();
        try {
            for (int i = 0; i < 10; i++) {
                final JsonObject data = new JsonObject();
                data.addProperty("id", "" + i);
                data.addProperty(MockTestBase.MOCK_PARTITION_FIELD_NAME, "xyz" + i);
                final JsonNode doc = mapper.readTree(data.toString());
                mockData.add(doc);

            }

            Function<CosmosPagedFluxOptions, Flux<FeedResponse<JsonNode>>> optionsFluxFunction
                = new Function<CosmosPagedFluxOptions, Flux<FeedResponse<JsonNode>>>() {
                @Override
                public Flux<FeedResponse<JsonNode>> apply(final CosmosPagedFluxOptions pagedFluxOptions) {
                    FeedResponse<JsonNode> _mockPage = null;
                    Constructor<?> constructor = null;
                    try {
                        constructor = FeedResponse.class.getDeclaredConstructor(List.class, Map.class);
                        constructor.setAccessible(true);
                        Map<String, String> mockHeader = (Map<String, String>) mock(Map.class);
                        _mockPage = (FeedResponse<JsonNode>) constructor.newInstance(mockData, mockHeader);
                    }catch (final Exception e) {
                        e.printStackTrace();
                    }
                    return Flux.just(_mockPage);

                }
            };
            Constructor<?> pagedFluxContructors = CosmosPagedFlux.class.getDeclaredConstructor(Function.class);
            pagedFluxContructors.setAccessible(true);
            CosmosPagedFlux<JsonNode> cosmosPagedFlux =  (CosmosPagedFlux<JsonNode>) pagedFluxContructors.newInstance(optionsFluxFunction);
            Constructor<?> pagedInterableConstructor = CosmosPagedIterable.class.getDeclaredConstructor(ContinuablePagedFlux.class);
            pagedInterableConstructor.setAccessible(true);
            cosmosPagedIterable = (CosmosPagedIterable<JsonNode>) pagedInterableConstructor.newInstance(cosmosPagedFlux);
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return cosmosPagedIterable;
    }
}