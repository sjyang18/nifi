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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.azure.cosmos.CosmosClientException;

@EventDriven
@Tags({ "azure", "cosmos", "insert", "record", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into CosmoDB with Core SQL API. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a flowfile and then inserts those records into " +
        "a configured Cosmos Container. This processor does not support updates, deletes or upserts.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutCosmosDocumentRecord extends AbstractCosmosDocumentProcessor {

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor INSERT_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("insert_batch_size")
        .displayName("Insert Batch Size")
        .description("The number of records to group together for one single insert operation against CosmosDB.")
        .defaultValue("100")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(RECORD_READER_FACTORY);
        _propertyDescriptors.add(INSERT_BATCH_SIZE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    protected void bulkInsert(List<Map<String, Object>> records ) throws CosmosClientException{
        // In the future, this method will be replaced by calling createItems API
        // for example, this.container.createItems(records);
        // currently, no createItems API available in Cosmos Java SDK
        for(Map<String, Object> record :  records){
            this.container.createItem(record);
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final String partitionKeyField = context.getProperty(PARTITION_KEY).getValue();
        List<Map<String, Object>> batch = new ArrayList<>();
        int ceiling = context.getProperty(INSERT_BATCH_SIZE).asInteger();
        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inStream, getLogger())) {

            RecordSchema schema = reader.getSchema();
            Record record;

            while((record = reader.nextRecord()) != null) {
                // Convert each Record to HashMap
                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(schema));
                if(contentMap.containsKey("id")) {
                    Object val = contentMap.get("id");
                    if(!(val instanceof String)) {
                        logger.debug("coverting number id into string...");
                        contentMap.put("id", contentMap.get("id").toString());
                    }
                } else {
                    contentMap.put("id", flowFile.getAttribute("uuid"));
                }
                if(!contentMap.containsKey(partitionKeyField)){
                    // Put record doesn't support update, so we set partition key field vaule
                    contentMap.put(partitionKeyField, flowFile.getAttribute("uuid"));
                }
                batch.add(contentMap);
                if (batch.size() == ceiling) {
                    bulkInsert(batch);
                    batch = new ArrayList<>();
                }
            }
            if(batch.size() > 0) {
                bulkInsert(batch);
            }
            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("Failed to createItem {} into CosmosDB due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        // No-Op  as of now, since Put does not need to warmup connection for initial performance gain
    }


}
