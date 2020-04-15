/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.azure.cosmos.document;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.azure.cosmos.CosmosPagedIterable;
import com.azure.cosmos.models.FeedOptions;

@Tags({ "azure", "cosmos", "document", "read", "get" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from documents in Cosmos Document DB loaded by a user-specified query (i.e. sql-like query).")
public class GetCosmosDocument extends AbstractCosmosDocumentProcessor {
    public static final PropertyDescriptor SEND_EMPTY_RESULTS = new PropertyDescriptor.Builder()
            .name("get-cosmos-send-empty").displayName("Send Empty Result")
            .description("If a query executes successfully, but returns no results, send an empty JSON document "
                    + "signifying no result.")
            .allowableValues("true", "false").defaultValue("false").addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false).build();

    static final PropertyDescriptor USE_PRETTY_PRINTING = new PropertyDescriptor.Builder().name("use-pretty-printing")
            .displayName("Pretty Print Results JSON")
            .description("Choose whether or not to pretty print the JSON from the results of the query. "
                    + "Choosing 'true' can greatly increase the space requirements on disk depending on the complexity of the JSON document")
            .required(true).defaultValue("false").allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("SQL Core Document Query")
            .description("The SQL select query to execute. "
                    + "This should be a valid SQL select query to cosmo document database with core sql api.")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    static final PropertyDescriptor MAX_RESPONSE_PAGE_SIZE = new PropertyDescriptor.Builder().name("max_page_size")
            .description("The maximum number of elements in a response page from cosmos document database")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder().name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description(
                    "How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.")
            .required(false).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;
    private ComponentLog logger;

    static {
        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(USE_PRETTY_PRINTING);
        _propertyDescriptors.add(CHARACTER_SET);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(MAX_RESPONSE_PAGE_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SEND_EMPTY_RESULTS);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private boolean sendEmpty;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> result = super.customValidate(context);

        final boolean queryIsSet = context.getProperty(QUERY).isSet();
        if (!queryIsSet) {
            final String msg = GetCosmosDocument.QUERY.getDisplayName() + " must be set.";
            result.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        return result;
    }

    // Turn a list of Cosmos result documents into a String representation of a JSON
    // array
    private String buildBatch(final List<JsonNode> documents, final boolean indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append("[");
        if (documents != null) {
            for (int index = 0; index < documents.size(); index++) {
                final JsonNode document = documents.get(index);
                String asJson;
                if (indent) {
                    asJson = document.toPrettyString();
                } else {
                    asJson = document.toString();
                }
                builder.append(asJson).append((index < documents.size() - 1) ? ", " : "");
            }
            documents.clear();
        }
        builder.append("]");
        return builder.toString();
    }

    private String getQuery(final ProcessContext context, final ProcessSession session, final FlowFile input) {
        String query = null;

        if (context.getProperty(QUERY).isSet()) {
            query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
        } else if (!context.getProperty(QUERY).isSet() && input == null) {
            query = "select top 100 * from c";
        }
        return query;
    }

    private Map<String, String> getAttributes(final ProcessContext context, final FlowFile input) {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        if (context.getProperty(QUERY).isSet()) {
            final String query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
            attributes.put("query", query);
        }
        return attributes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        sendEmpty = context.getProperty(SEND_EMPTY_RESULTS).asBoolean();
        final FlowFile input = context.hasIncomingConnection() ? session.get() : null;
        logger = getLogger();
        logger.debug("inside GetCosmosDocument.onTrigger");

        if (input == null && context.hasNonLoopConnection()) {
            return;
        }
        final String query;
        final Map<String, String> attributes;
        try {
            query = getQuery(context, session, input);
            attributes = getAttributes(context, input);
        } catch (final Exception ex) {
            logger.error("Error parsing query or getting attirbutes.", ex);
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
            return; // We need to stop immediately.
        }

        final boolean usePrettyPrint = context.getProperty(USE_PRETTY_PRINTING).asBoolean();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

        final FeedOptions queryOptions = new FeedOptions();

        if (context.getProperty(MAX_RESPONSE_PAGE_SIZE).isSet()) {
            final int max_page_size = context.getProperty(MAX_RESPONSE_PAGE_SIZE).evaluateAttributeExpressions(input)
                    .asInteger();
            logger.debug("setting max page size : " + max_page_size);
            queryOptions.setMaxItemCount(max_page_size);
        }
        logger.debug("Running Cosmos SQL query : " + query);

        final CosmosPagedIterable<JsonNode> pages = container != null
                ? container.queryItems(query, queryOptions, JsonNode.class)
                : null;

        if (pages == null) {
            logger.error("Fails to get CosmosPagedIterable<JsonNode> Iterator");
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
            return;
        }

        try {
            logger.debug("Start to processing data");
            final AtomicBoolean sent = new AtomicBoolean(false);
            if (context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                logger.debug("Sending out results in batch mode");
                final int sizePerBatch = context.getProperty(RESULTS_PER_FLOWFILE).asInteger();
                final List<JsonNode> batch = new ArrayList<>();
                pages.iterableByPage(sizePerBatch).forEach(feedResponse -> {
                    for (final JsonNode doc : feedResponse.getResults()) {
                        batch.add(doc);
                        if (batch.size() == sizePerBatch) {
                            writeBatch(buildBatch(batch, usePrettyPrint), input, context, session, attributes,
                                    REL_SUCCESS);
                            sent.set(true);
                        }
                    }
                });
                if (batch.size() > 0) {
                    writeBatch(buildBatch(batch, usePrettyPrint), input, context, session, attributes, REL_SUCCESS);
                    sent.set(true);
                }
            } else {
                logger.debug("Sending out results one by one");
                pages.iterableByPage(10).forEach(feedResponse -> {
                    for (final JsonNode doc : feedResponse.getResults()) {
                        FlowFile outgoingFlowFile = (input == null) ? session.create() : session.create(input);
                        outgoingFlowFile = session.write(outgoingFlowFile, out -> {
                            if (usePrettyPrint) {
                                out.write(doc.toPrettyString().getBytes(charset));
                            } else {
                                out.write(doc.toString().getBytes(charset));
                            }
                        });
                        if (attributes != null) {
                            outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes);
                        }
                        session.getProvenanceReporter().receive(outgoingFlowFile, getURI(context));
                        session.transfer(outgoingFlowFile, REL_SUCCESS);
                        sent.set(true);
                    }
                });
                logger.debug("ending outputing records in one-by-one mode");
            }
            if (!sent.get() && sendEmpty) {
                FlowFile empty = input != null ? session.create(input) : session.create();
                empty = session.putAllAttributes(empty, attributes);
                session.transfer(empty, REL_SUCCESS);
            }
            logger.debug("ending handling pages of documents");

        } catch (final Exception e) {
            logger.error("Failed to be completed with: " + e.getMessage(), e);
        }
    }

    private void warmupCosmosContainer() {
        // This method will be invoked once during OnSchedule so that actual queries, run during
        // onTrigger, perform fasters
        // With this method, the intial first query may avoid the noticable initial
        // delay.
        if (cosmosClient != null) {
            final FeedOptions queryOptions = new FeedOptions();
            queryOptions.setMaxItemCount(10);
            container.queryItems("select top 1 c.id from c", queryOptions, JsonNode.class);
        }
    }

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        try {
            warmupCosmosContainer();
        } catch (final Exception e) {
            logger.error("failure in doPostActionOnSchedule with "+ e.getMessage(), e);
        }
    }
}
