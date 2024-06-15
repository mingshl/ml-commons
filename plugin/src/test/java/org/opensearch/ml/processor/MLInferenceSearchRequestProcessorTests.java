/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.ml.processor.MLInferenceSearchRequestProcessor.DEFAULT_MAX_PREDICTION_TASKS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

public class MLInferenceSearchRequestProcessorTests extends AbstractBuilderTestCase {

    @Mock
    private Client client;
    static public final NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_QUERY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );
    private static final String PROCESSOR_TAG = "inference";
    private static final String DESCRIPTION = "inference_test";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * When no mapping is provided,
     * TODO expected behavior ? throw Exceptions
     * @throws Exception
     */
    public void testProcessRequestWithNoMappings() throws Exception {

        MLInferenceSearchRequestProcessor requestProcessor = new MLInferenceSearchRequestProcessor(
            "model1",
            null,
            null,
            null,
            null,
            DEFAULT_MAX_PREDICTION_TASKS,
            PROCESSOR_TAG,
            DESCRIPTION,
            false,
            "remote",
            false,
            false,
            "{ \"parameters\": ${ml_inference.parameters} }",
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY
        );
        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);
        SearchRequest transformedRequest = requestProcessor.processRequest(request);
        assertEquals(incomingQuery, transformedRequest.source().query());
    }

    public void testExecute_rewriteSingleStringTermQuerySuccess() throws Exception {

        /**
         * example term query: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
         */
        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "query.term.text.value";
        String modelOutputField = "response";
        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            null,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );
        // TODO test list
        // ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3))).build();
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", "eng")).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest newSearchRequest = requestProcessor.processRequest(request);
        QueryBuilder expectedQuery = new TermQueryBuilder("text", "eng");
        /**
         * example term query: {"query":{"term":{"text":{"value":"eng","boost":1.0}}}}
         */
        assertEquals(expectedQuery, newSearchRequest.source().query());
        assertEquals(request.toString(), newSearchRequest.toString());
    }

    public void testExecute_rewriteMultipleStringTermQuerySuccess() throws Exception {

        /**
         * example term query: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
         */
        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "query.term.text.value";
        String modelOutputField = "response";
        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            null,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", "car, truck, vehicle")).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest newSearchRequest = requestProcessor.processRequest(request);
        /**
         * example term query: {"query":{"term":{"text":{"value":"car, truck, vehicle","boost":1.0}}}}
         */
        QueryBuilder expectedQuery = new TermQueryBuilder("text", "car, truck, vehicle");
        assertEquals(expectedQuery, newSearchRequest.source().query());
        assertEquals(request.toString(), newSearchRequest.toString());
    }

    public void testExecute_rewriteDoubleQuerySuccess() throws Exception {

        /**
         * example term query: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
         */
        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "query.term.text.value";
        String modelOutputField = "response";
        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            null,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );

        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", 0.123)).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        /**
         * example term query: {"query":{"term":{"text":{"value":0.123,"boost":1.0}}}}
         */
        SearchRequest newSearchRequest = requestProcessor.processRequest(request);
        QueryBuilder expectedQuery = new TermQueryBuilder("text", 0.123);
        assertEquals(expectedQuery, newSearchRequest.source().query());
        assertEquals(request.toString(), newSearchRequest.toString());
    }

    public void testExecute_rewriteStringFromTermQueryToRangeQuerySuccess() throws Exception {
        /**
         * example term query: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
         */
        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "modelPredictionScore";
        String modelOutputField = "response";
        String queryTemplate = "{\"query\":{\"range\":{\"text\":{\"gte\":\"${modelPredictionScore}\"}}}}";

        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            queryTemplate,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );

        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", "0.123")).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        /**
         * example input term query: {"query":{"term":{"text":{"value":foo,"boost":1.0}}}}
         */
        /**
         * example output range query: {"query":{"range":{"text":{"gte":"2"}}}}
         */

        SearchRequest newSearchRequest = requestProcessor.processRequest(request);
        RangeQueryBuilder expectedQuery = new RangeQueryBuilder("text");
        expectedQuery.from(0.123);
        expectedQuery.includeLower(true);
        assertEquals(expectedQuery, newSearchRequest.source().query());
        assertEquals(request.toString(), newSearchRequest.toString());
    }

    public void testExecute_rewriteDoubleFromTermQueryToRangeQuerySuccess() throws Exception {
        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "modelPredictionScore";
        String modelOutputField = "response";
        String queryTemplate = "{\"query\":{\"range\":{\"text\":{\"gte\":\"${modelPredictionScore}\"}}}}";

        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            queryTemplate,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );

        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", 0.123)).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        /**
         * example input term query: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
         */
        /**
         * example output range query: {"query":{"range":{"text":{"gte":0.123}}}}
         */

        SearchRequest newSearchRequest = requestProcessor.processRequest(request);
        RangeQueryBuilder expectedQuery = new RangeQueryBuilder("text");
        expectedQuery.from(0.123);
        expectedQuery.includeLower(true);
        assertEquals(expectedQuery, newSearchRequest.source().query());
        assertEquals(request.toString(), newSearchRequest.toString());
    }

    public void testExecute_rewriteListFromTermQueryToGeometryQuerySuccess() throws Exception {

        String queryTemplate = "{\n"
            + "  \"query\": {\n"
            + "  \"geo_shape\" : {\n"
            + "    \"location\" : {\n"
            + "      \"shape\" : {\n"
            + "        \"type\" : \"Envelope\",\n"
            + "        \"coordinates\" : \"${modelPredictionOutcome}\" \n"
            + "      },\n"
            + "      \"relation\" : \"intersects\"\n"
            + "    },\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "  }\n"
            + "}";

        String expectedNewQueryString = "{\n"
            + "  \"query\": {\n"
            + "  \"geo_shape\" : {\n"
            + "    \"location\" : {\n"
            + "      \"shape\" : {\n"
            + "        \"type\" : \"Envelope\",\n"
            + "        \"coordinates\" : [ [ 0.0, 6.0], [ 4.0, 2.0] ]\n"
            + "      },\n"
            + "      \"relation\" : \"intersects\"\n"
            + "    },\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "  }\n"
            + "}";

        String modelInputField = "inputs";
        String originalQueryField = "query.term.text.value";
        String newQueryField = "modelPredictionOutcome";
        String modelOutputField = "response";
        MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
            queryTemplate,
            modelInputField,
            originalQueryField,
            newQueryField,
            modelOutputField
        );
        ModelTensor modelTensor = ModelTensor
            .builder()
            .dataAsMap(ImmutableMap.of("response", Arrays.asList(Arrays.asList(0.0, 6.0), Arrays.asList(4.0, 2.0))))
            .build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        QueryBuilder incomingQuery = new TermQueryBuilder("text", "Seattle");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest newSearchRequest = requestProcessor.processRequest(request);

        XContentParser parser = createParser(JsonXContent.jsonXContent, expectedNewQueryString);
        SearchSourceBuilder expectedSearchSourceBuilder = new SearchSourceBuilder();
        expectedSearchSourceBuilder.parseXContent(parser);
        SearchRequest expectedRequest = new SearchRequest().source(expectedSearchSourceBuilder);

        assertEquals(expectedRequest.source().query(), newSearchRequest.source().query());
        System.out.println(newSearchRequest.source().query());
        assertEquals(expectedRequest.toString(), newSearchRequest.toString());
    }

    // public void testNullQueryStringException() throws Exception {
    // String modelInputField = "inputs";
    // String originalQueryField = "query.term.text.value";
    // String newQueryField = "modelPredictionScore";
    // String modelOutputField = "response";
    // String queryTemplate = "";
    //
    // MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
    // queryTemplate,
    // modelInputField,
    // originalQueryField,
    // newQueryField,
    // modelOutputField
    // );
    // SearchRequest request = new SearchRequest();
    // try{
    // SearchRequest newSearchRequest = requestProcessor.processRequest(request);
    // }
    // catch (Exception e){
    // assertEquals("this method should not get executed.", e.getMessage());
    // }
    // }
    // public void testNullQueryStringIgnoreFailure() throws Exception {
    // String modelInputField = "inputs";
    // String originalQueryField = "query.term.text.value";
    // String newQueryField = "modelPredictionScore";
    // String modelOutputField = "response";
    // String queryTemplate = "";
    //
    // MLInferenceSearchRequestProcessor requestProcessor = getMlInferenceSearchRequestProcessorTermQuery(
    // queryTemplate,
    // modelInputField,
    // originalQueryField,
    // newQueryField,
    // modelOutputField
    // );
    // SearchRequest request = new SearchRequest();
    // try{
    // SearchRequest newSearchRequest = requestProcessor.processRequest(request);
    // }
    // catch (Exception e){
    // assertEquals("this method should not get executed.", e.getMessage());
    // }
    // }
    public void testExecutePrint() throws Exception {
        String queryTemplate = "{\n"
            + "  \"query\": {\n"
            + "  \"geo_shape\" : {\n"
            + "    \"location\" : {\n"
            + "      \"shape\" : {\n"
            + "        \"type\" : \"Envelope\",\n"
            + "        \"coordinates\" : ${modelPredictionOutcome} \n"
            + "      },\n"
            + "      \"relation\" : \"intersects\"\n"
            + "    },\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "  }\n"
            + "}";

        String expectedNewQueryString = "{\n"
            + "  \"query\": {\n"
            + "  \"geo_shape\" : {\n"
            + "    \"location\" : {\n"
            + "      \"shape\" : {\n"
            + "        \"type\" : \"Envelope\",\n"
            + "        \"coordinates\" : [ [ 0.0, 6.0], [ 4.0, 2.0] ]\n"
            + "      },\n"
            + "      \"relation\" : \"intersects\"\n"
            + "    },\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "  }\n"
            + "}";

        System.out.println(queryTemplate);
        System.out.println(expectedNewQueryString);
    }

    private MLInferenceSearchRequestProcessor getMlInferenceSearchRequestProcessorTermQuery(
        String queryTemplate,
        String modelInputField,
        String originalQueryField,
        String newQueryField,
        String modelOutputField
    ) {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        input.put(modelInputField, originalQueryField);
        inputMap.add(input);

        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put(newQueryField, modelOutputField);
        outputMap.add(output);

        MLInferenceSearchRequestProcessor requestProcessor = new MLInferenceSearchRequestProcessor(
            "model1",
            queryTemplate,
            inputMap,
            outputMap,
            null,
            DEFAULT_MAX_PREDICTION_TASKS,
            PROCESSOR_TAG,
            DESCRIPTION,
            false,
            "remote",
            false,
            false,
            "{ \"parameters\": ${ml_inference.parameters} }",
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY
        );
        return requestProcessor;
    }

}
