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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.MockFlowFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.azure.cosmos.CosmosClientException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class ITGetAzureCosmosDBDocument extends ITAbstractAzureCosmosDBDocument {

    private static final String TEST_COSMOS_QUERY = "select top 100 * from c";
    private static List<JsonNode> testData;
    private static int numOfTestData = 10;


    static {
        final ObjectMapper mapper = new ObjectMapper();
        testData = new ArrayList<>();
        JsonNode doc = null;
        for(int i=0; i< numOfTestData; i++){
            JsonObject json =  new JsonObject();
            json.addProperty("id", ""+i);
            json.addProperty(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "xyz"+i);
            try {
                doc = mapper.readTree(json.toString());
            } catch(IOException exp) {
                exp.printStackTrace();
            }
            testData.add(doc);
        }

        for(JsonNode eachdoc : testData){
            try {
                container.upsertItem(eachdoc);
            }catch(CosmosClientException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return GetAzureCosmosDBDocument.class;
    }
    @Before
    public void setQuery() {
        runner.setProperty(GetAzureCosmosDBDocument.QUERY, TEST_COSMOS_QUERY);
    }
    @After
    public void resetGetTestSpecificProperties() {
        runner.removeProperty(GetAzureCosmosDBDocument.RESULTS_PER_FLOWFILE);
    }

    @Test
    public void testProcessorConfigValidity() {
        runner.run();
    }

    @Test
    public void testProcessorConfigValidityWithConnectionServiceController() throws Exception {
        this.configureCosmosConnectionControllerService();
        runner.run();
    }
    @Test
    public void testReadDocuments() {
        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(GetAzureCosmosDBDocument.REL_SUCCESS, testData.size());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetAzureCosmosDBDocument.REL_SUCCESS);
        for(int idx=0; idx < testData.size();  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    @Test
    public void testMultipleResultsInFlowFile() {
        JsonParser parser = new JsonParser();
        runner.setProperty(GetAzureCosmosDBDocument.RESULTS_PER_FLOWFILE, "3");
        runner.enqueue(new byte[] {});
        runner.run();

        int expectedNumOfFlowFiles = (int) Math.ceil(testData.size()/3.0);

        runner.assertAllFlowFilesTransferred(GetAzureCosmosDBDocument.REL_SUCCESS, expectedNumOfFlowFiles); // 4 flowfils with each having [3,3,3,1] records.
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetAzureCosmosDBDocument.REL_SUCCESS);
        for(int idx=0; idx < expectedNumOfFlowFiles;  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
            JsonArray jArray = parsedJson.getAsJsonArray();
            if (idx < expectedNumOfFlowFiles-1) {
               assertEquals(3, jArray.size());  // all other than the last flow file should have 3 Json records
            }
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }

    }

    @Test
    public void testMultipleResultsInFlowFileWithResultSizeSet() {
        JsonParser parser = new JsonParser();
        runner.setProperty(GetAzureCosmosDBDocument.RESULTS_PER_FLOWFILE, "3");
        runner.enqueue(new byte[] {});
        runner.run();

        int expectedNumOfFlowFiles = (int) Math.ceil(testData.size()/3.0);

        runner.assertAllFlowFilesTransferred(GetAzureCosmosDBDocument.REL_SUCCESS, expectedNumOfFlowFiles); // 4 flowfils with each having [3,3,3,1] records.
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetAzureCosmosDBDocument.REL_SUCCESS);
        for(int idx=0; idx < expectedNumOfFlowFiles;  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
            JsonArray jArray = parsedJson.getAsJsonArray();
            if (idx < expectedNumOfFlowFiles-1) {
               assertEquals(3, jArray.size());  // all other than last flow file should have 3 Json records
            }
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }

    }
 }