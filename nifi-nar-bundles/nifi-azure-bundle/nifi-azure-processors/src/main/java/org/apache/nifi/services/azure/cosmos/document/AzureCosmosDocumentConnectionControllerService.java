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

package org.apache.nifi.services.azure.cosmos.document;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDocumentConnectionService;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;

@Tags({"azure", "cosmos", "document", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Azure Cosmos Document DB (Document API or Core SQL API recently renamed) " +
                " and provides access to that connection to other CosmosDB-related components."
)
public class AzureCosmosDocumentConnectionControllerService extends AbstractControllerService implements AzureCosmosDocumentConnectionService {
    static final String CONSISTENCY_STRONG = "STRONG";
    static final String CONSISTENCY_BOUNDED_STALENESS= "BOUNDED_STALENESS";
    static final String CONSISTENCY_SESSION = "SESSION";
    static final String CONSISTENCY_CONSISTENT_PREFIX = "CONSISTENT_PREFIX";
    static final String CONSISTENCY_EVENTUAL = "EVENTUAL";
    private String uri;
    private String accessKey;
    protected CosmosClient cosmosClient;

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
    .name("Cosmos Document DB URI")
    .displayName("Cosmos Document DB URI")
    .description("CosmosURI, typically of the form: https://{databaseaccount}.documents.azure.com:443/. Note this URI is for Cosmos DB with SQL API")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_ACCESS_KEY = new PropertyDescriptor.Builder()
        .name("Cosmos Document DB Access Key")
        .displayName("Cosmos Document DB Access Key")
        .description("Cosmos DB Access Key from Azure Portal (Settings->Keys)")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor CONSISTENCY = new PropertyDescriptor.Builder()
        .name("Cosmos Consistency Level")
        .displayName("Cosmos Consistency Level")
        .description("Cosmos Consistency Level to use")
        .required(false)
        .allowableValues(CONSISTENCY_STRONG, CONSISTENCY_BOUNDED_STALENESS, CONSISTENCY_SESSION,
                CONSISTENCY_CONSISTENT_PREFIX, CONSISTENCY_EVENTUAL)
        .defaultValue(CONSISTENCY_SESSION)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uri = context.getProperty(URI).getValue();
        this.accessKey = context.getProperty(DB_ACCESS_KEY).getValue();
        final ConsistencyLevel clevel;
        final String selectedConsistency;
        if(context.getProperty(CONSISTENCY).isSet()){
            selectedConsistency = context.getProperty(CONSISTENCY).getValue();
        }else {
            selectedConsistency =  CONSISTENCY_SESSION;
        }

        switch(selectedConsistency) {
            case CONSISTENCY_STRONG:
                clevel =  ConsistencyLevel.STRONG;
                break;
            case CONSISTENCY_CONSISTENT_PREFIX:
                clevel = ConsistencyLevel.CONSISTENT_PREFIX;
                break;
            case CONSISTENCY_SESSION:
                clevel = ConsistencyLevel.SESSION;
                break;
            case CONSISTENCY_BOUNDED_STALENESS:
                clevel = ConsistencyLevel.BOUNDED_STALENESS;
                break;
            case CONSISTENCY_EVENTUAL:
                clevel = ConsistencyLevel.EVENTUAL;
                break;
            default:
                clevel = ConsistencyLevel.SESSION;
        }

        if (cosmosClient != null) {
            closeClient();
        }
        createDocumentClient(uri, accessKey, clevel);
    }


    @OnStopped
    public final void closeClient() {
        if (this.cosmosClient != null) {
            cosmosClient.close();
            cosmosClient = null;
        }
    }

    protected void createDocumentClient(final String uri, final String accessKey, final ConsistencyLevel clevel){
        try {
            ConnectionPolicy connectionPolicy = ConnectionPolicy.getDefaultPolicy();
            this.cosmosClient = new CosmosClientBuilder()
            .setEndpoint(uri)
            .setKey(accessKey)
            .setConnectionPolicy(connectionPolicy).setConsistencyLevel(clevel)
            .buildClient();
        } catch (Exception e) {
            getLogger().error("Failed to build cosmosClient {} due to {}", new Object[] { this.getClass().getName(), e }, e);
        }
    }

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(URI);
        descriptors.add(DB_ACCESS_KEY);
        descriptors.add(CONSISTENCY);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public String getURI() {
        return this.uri;
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }

    @Override
    public CosmosClient getCosmosClient(){
        return this.cosmosClient;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String uri = validationContext.getProperty(AzureCosmosDocumentConnectionControllerService.URI).getValue();
        final String db_access_key = validationContext.getProperty(AzureCosmosDocumentConnectionControllerService.DB_ACCESS_KEY).getValue();

        if (StringUtils.isBlank(uri) || StringUtils.isBlank(db_access_key)) {
            results.add(new ValidationResult.Builder().subject("AzureStorageCredentialsControllerService")
                    .valid(false)
                    .explanation(
                        "either " + AzureCosmosDocumentConnectionControllerService.URI.getDisplayName()
                        + " or " + AzureCosmosDocumentConnectionControllerService.DB_ACCESS_KEY.getDisplayName() + " is required")
                    .build());
        }
        return results;
    }

}
