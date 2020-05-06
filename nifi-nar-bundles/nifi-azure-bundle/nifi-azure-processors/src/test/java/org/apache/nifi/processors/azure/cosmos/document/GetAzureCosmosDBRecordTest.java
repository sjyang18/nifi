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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.azure.core.util.paging.ContinuablePagedFlux;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainer;
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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import reactor.core.publisher.Flux;


public class GetAzureCosmosDBRecordTest extends MockTestBase {


    public static final String MOCK_QUERY2 = "select * from d";
    public static final int MOCK_DATA_NUM = 10;
    private MockGetAzureCosmosDBRecord processor;

    private static RecordSchema SCHEMA1;
    private static RecordSchema SCHEMA2;

    static {
        final List<RecordField> testDataFields = new ArrayList<>();
        final RecordField idField = new RecordField("id", RecordFieldType.STRING.getDataType());
        final RecordField categoryField = new RecordField(MOCK_PARTITION_FIELD_NAME, RecordFieldType.INT.getDataType());
        final RecordField payloadField = new RecordField("payload", RecordFieldType.STRING.getDataType());
        testDataFields.add(idField);
        testDataFields.add(categoryField);
        testDataFields.add(payloadField);
        SCHEMA1 = new SimpleRecordSchema(testDataFields);

        final List<RecordField> testDataFields2 = new ArrayList<>();
        final RecordField payloadField2 = new RecordField("payload", RecordFieldType.ARRAY.getDataType());
        testDataFields.add(idField);
        testDataFields.add(categoryField);
        testDataFields.add(payloadField2);
        SCHEMA2 = new SimpleRecordSchema(testDataFields2);
    }

    @Before
    public void setUp() throws Exception {
        processor = new MockGetAzureCosmosDBRecord();
        testRunner = TestRunners.newTestRunner(processor);

        // setup  schema registry, record writer, and schema for test cases
        MockSchemaRegistry registry = new MockSchemaRegistry();
        JsonRecordSetWriter writer = new JsonRecordSetWriter();

        registry.addSchema("sample", SCHEMA1);
        registry.addSchema("sample2", SCHEMA2);
        testRunner.addControllerService("registry", registry);
        testRunner.enableControllerService(registry);

        testRunner.addControllerService("writer", writer);
        testRunner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        testRunner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        testRunner.enableControllerService(writer);
        testRunner.setProperty(MockGetAzureCosmosDBRecord.WRITER_FACTORY, "writer");
    }


    @Test
    public void testProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.setProperty(GetAzureCosmosDBRecord.URI,MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBRecord.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testReadRecords() throws Exception {
        int numOfTestData = GetAzureCosmosDBRecordTest.MOCK_DATA_NUM;
        prepareMockProcess();
        testRunner.assertValid();

        JsonParser parser = new JsonParser();
        testRunner.setVariable("schema.name", "sample");
        testRunner.enqueue(new byte[] {});
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetAzureCosmosDBRecord.REL_SUCCESS);
        assertTrue(flowFiles.size() == 1);
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("record.count", String.valueOf(numOfTestData));

        JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
        JsonArray jArray = parsedJson.getAsJsonArray();
        assertTrue(jArray.size() == numOfTestData);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_ORIGINAL, 1);
    }

    @Test
    public void testReadRecordWithArrayPayload() throws Exception {
        int numOfTestData = GetAzureCosmosDBRecordTest.MOCK_DATA_NUM;
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY,MOCK_QUERY2);

        prepareMockProcess();
        testRunner.assertValid();

        JsonParser parser = new JsonParser();
        testRunner.setVariable("schema.name", "sample2");
        testRunner.enqueue(new byte[] {});
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetAzureCosmosDBRecord.REL_SUCCESS);
        assertTrue(flowFiles.size() == 1);
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("record.count", String.valueOf(numOfTestData));

        JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
        JsonArray jArray = parsedJson.getAsJsonArray();
        assertTrue(jArray.size() == numOfTestData);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetAzureCosmosDBRecord.REL_ORIGINAL, 1);
    }

    private void prepareMockProcess() throws Exception{
        // this setup connection service and basic mock properties
        this.setBasicMockProperties(true);
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY, MOCK_QUERY);

    }


}


@SuppressWarnings("unchecked")
class MockGetAzureCosmosDBRecord extends GetAzureCosmosDBRecord {

    List<JsonNode> _mockData1;
    List<JsonNode> _mockData2;


    CosmosPagedIterable<JsonNode> mockPagedIterable;
    final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void createCosmosClient(String uri, String accessKey, ConsistencyLevel clevel) throws NullPointerException, CosmosClientException {
        // create a mock DocumentClient
        this.cosmosClient = mock(CosmosClient.class);

    }

    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws ProcessException {
        this.container = mock(CosmosContainer.class);
        when(this.container.queryItems(eq(GetAzureCosmosDBRecordTest.MOCK_QUERY), any(), eq(JsonNode.class))).thenReturn(mockResponseData1());
        when(this.container.queryItems(eq(GetAzureCosmosDBRecordTest.MOCK_QUERY2), any(), eq(JsonNode.class))).thenReturn(mockResponseData2());

    }



    private CosmosPagedIterable<JsonNode> mockResponseData1(){

        CosmosPagedIterable<JsonNode> cosmosPagedIterable =null;
        try {
            _mockData1 = new ArrayList<>();
            for(int i=0; i< GetAzureCosmosDBRecordTest.MOCK_DATA_NUM; i++){
                JsonObject json =  new JsonObject();
                json.addProperty("id", ""+i);
                json.addProperty(GetAzureCosmosDBRecordTest.MOCK_PARTITION_FIELD_NAME, MockTestBase.getRandomInt(1,4));
                json.addProperty("payload", RandomStringUtils.random(100, true, true));
                final JsonNode doc = mapper.readTree(json.toString());
                _mockData1.add(doc);
            }

            cosmosPagedIterable = convertMockReturn(_mockData1);
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return cosmosPagedIterable;
    }

    private CosmosPagedIterable<JsonNode> mockResponseData2(){
        CosmosPagedIterable<JsonNode> cosmosPagedIterable =null;
        try{
            _mockData2 = new ArrayList<>();
            for(int i=0; i< GetAzureCosmosDBRecordTest.MOCK_DATA_NUM; i++){
                JsonObject json =  new JsonObject();
                json.addProperty("id", ""+i);
                json.addProperty(GetAzureCosmosDBRecordTest.MOCK_PARTITION_FIELD_NAME, MockTestBase.getRandomInt(1,4));
                JsonArray  arr = new JsonArray();
                for(int j= 0; j< i; j++){
                    arr.add(j);
                }
                json.add("payload", arr);
                final JsonNode doc =  mapper.readTree(json.toString());
                _mockData2.add(doc);
            }
            cosmosPagedIterable = convertMockReturn(_mockData2);

        }catch (final Exception e) {
            e.printStackTrace();
        }
        return cosmosPagedIterable;
    }

    private CosmosPagedIterable<JsonNode> convertMockReturn(final List<JsonNode> data)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        final CosmosPagedIterable<JsonNode> cosmosPagedIterable;
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
                    _mockPage = (FeedResponse<JsonNode>) constructor.newInstance(data, mockHeader);
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
        return cosmosPagedIterable;
    }

}