/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.ml.processor;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.*;

import java.io.IOException;
import java.util.*;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;

/**
 * MLInferenceSearchRequestProcessor requires a modelId string to call model inferences
 * maps fields in document for model input, and maps model inference output to the query fields
 * this processor also handles dot path notation for nested object( map of array) by rewriting json path accordingly
 */
public class MLInferenceSearchRequestProcessor extends AbstractProcessor implements SearchRequestProcessor, ModelExecutor {
    private final NamedXContentRegistry xContentRegistry;
    private static final Logger logger = LogManager.getLogger(MLInferenceSearchRequestProcessor.class);
    private final InferenceProcessorAttributes inferenceProcessorAttributes;
    private final boolean ignoreMissing;
    private final String functionName;
    private String queryTemplate;
    private final boolean fullResponsePath;
    private final boolean ignoreFailure;
    private final String modelInput;
    private static Client client;
    public static final String TYPE = "ml_inference";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";
    // allow to ignore a field from mapping is not present in the document, and when the outfield is not found in the
    // prediction outcomes, return the whole prediction outcome by skipping filtering
    public static final String IGNORE_MISSING = "ignore_missing";
    public static final String QUERY_TEMPLATE = "query_template";
    public static final String FUNCTION_NAME = "function_name";
    public static final String FULL_RESPONSE_PATH = "full_response_path";
    public static final String MODEL_INPUT = "model_input";
    // At default, ml inference processor allows maximum 10 prediction tasks running in parallel
    // it can be overwritten using max_prediction_tasks when creating processor
    public static final int DEFAULT_MAX_PREDICTION_TASKS = 10;

    protected MLInferenceSearchRequestProcessor(
        String modelId,
        String queryTemplate,
        List<Map<String, String>> inputMaps,
        List<Map<String, String>> outputMaps,
        Map<String, String> modelConfigMaps,
        int maxPredictionTask,
        String tag,
        String description,
        boolean ignoreMissing,
        String functionName,
        boolean fullResponsePath,
        boolean ignoreFailure,
        String modelInput,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(tag, description, ignoreFailure);
        this.inferenceProcessorAttributes = new InferenceProcessorAttributes(
            modelId,
            inputMaps,
            outputMaps,
            modelConfigMaps,
            maxPredictionTask
        );
        this.ignoreMissing = ignoreMissing;
        this.functionName = functionName;
        this.fullResponsePath = fullResponsePath;
        this.queryTemplate = queryTemplate;
        this.ignoreFailure = ignoreFailure;
        this.modelInput = modelInput;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Process a SearchRequest without receiving request-scoped state.
     * Implement this method if the processor makes no asynchronous calls.
     *
     * @param request the search request (which may have been modified by an earlier processor)
     * @return the modified search request
     * @throws Exception implementation-specific processing exception
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        // handle null query to avoid further processing
        try {
            if (request.source() == null) {
                return request.source(new SearchSourceBuilder());
            }

            String queryString = request.source().toString();

            /**
             * example: {"query":{"term":{"text":{"value":"foo","boost":1.0}}}}
             */

            String newQuery = rewriteQueryString(queryString);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            XContentParser queryParser = XContentType.JSON
                .xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, newQuery);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, queryParser.nextToken(), queryParser);
            searchSourceBuilder.parseXContent(queryParser);
            request.source(searchSourceBuilder);
        } catch (Exception e) {
            if (ignoreFailure) {
                return request;
            } else {
                throw e;
            }
        }
        return request;
    }

    private String rewriteQueryString(String queryString) throws IOException {
        List<Map<String, String>> processInputMap = inferenceProcessorAttributes.getInputMaps();
        List<Map<String, String>> processOutputMap = inferenceProcessorAttributes.getOutputMaps();
        int inputMapSize = (processInputMap != null) ? processInputMap.size() : 0;

        if (inputMapSize == 0) {
            return queryString;
        }

        try {
            if (!validateQueryFieldInQueryString(processInputMap, processOutputMap, queryString)) {
                return queryString;
            }
        } catch (Exception e) {
            if (ignoreMissing) {
                return queryString;
            } else {
                throw new IllegalArgumentException("cannot find mappings in query string");
            }
        }

        Object incomeQueryObject = JsonPath.parse(queryString).read("$");
        Object queryTemplateObject = null;
        if (queryTemplate != null) {
            queryTemplateObject = StringUtils.gson.fromJson(queryTemplate, Map.class);
        }
        Object[] finalQueryTemplateObject = { queryTemplateObject };

        // Action Future
        ActionListener<Map<Integer, MLOutput>> queryListener = new ActionListener<>() {

            @Override
            public void onResponse(Map<Integer, MLOutput> multipleMLOutputs) {
                for (Map.Entry<Integer, MLOutput> entry : multipleMLOutputs.entrySet()) {
                    Integer mappingIndex = entry.getKey();
                    MLOutput mlOutput = entry.getValue();
                    Map<String, String> outputMapping = processOutputMap.get(mappingIndex);

                    for (Map.Entry<String, String> outputMapEntry : outputMapping.entrySet()) {
                        // new query field as key, model field as value
                        String newQueryField = outputMapEntry.getKey();
                        String modelOutputFieldName = outputMapEntry.getValue();
                        Object modelOutputValue = getModelOutputValue(mlOutput, modelOutputFieldName, ignoreMissing, fullResponsePath);
                        // replace queryString by queryField using JsonPath with modelOutputValue
                        if (queryTemplate == null) {
                            // Create a JsonPath expression to replace the value of the queryField
                            String jsonPathExpression = "$." + newQueryField;

                            // Replace the value of the queryField with the modelOutputValue
                            JsonPath.parse(incomeQueryObject).set(jsonPathExpression, modelOutputValue);
                        } else {
                            // Build map
                            Map<String, Object> valuesMap = new HashMap<>();
                            valuesMap.put(newQueryField, modelOutputValue);

                            // Build StringSubstitutor
                            StringSubstitutor sub = new StringSubstitutor(valuesMap);
                            String getQueryString = finalQueryTemplateObject[0].toString();
                            // Replace
                            String newQueryString = sub.replace(getQueryString);

                            Object newQueryObject = StringUtils.gson.fromJson(newQueryString, Map.class);
                            finalQueryTemplateObject[0] = newQueryObject;
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Get Prediction Result Failed:", e);
            }
        };
        // ActionFuture
        GroupedActionListener<Map<Integer, MLOutput>> batchPredictionListener = createBatchPredictionListener(
            queryListener,
            processOutputMap,
            incomeQueryObject,
            inputMapSize
        );

        for (int inputMapIndex = 0; inputMapIndex < inputMapSize; inputMapIndex++) {
            try {
                processPredictions(queryString, processInputMap, processOutputMap, inputMapIndex, inputMapSize, batchPredictionListener);
            } catch (Exception e) {
                batchPredictionListener.onFailure(e);
                throw new RuntimeException("Exception during process prediction result: " + e.getMessage());
            }
        }
        // check if query template is not null and rewritten
        if (queryTemplate != null && toString(finalQueryTemplateObject[0]) != queryTemplate) {
            return toString(finalQueryTemplateObject[0]);
        } else {
            return toString(incomeQueryObject);
        }

    }

    private GroupedActionListener<Map<Integer, MLOutput>> createBatchPredictionListener(
        ActionListener<Map<Integer, MLOutput>> queryListener,
        List<Map<String, String>> processOutputMap,
        Object newQuery,
        int inputMapSize
    ) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<Map<Integer, MLOutput>> mlOutputMapCollection) {
                Map<Integer, MLOutput> mlOutputMaps = new HashMap<>();
                for (Map<Integer, MLOutput> mlOutputMap : mlOutputMapCollection) {
                    mlOutputMaps.putAll(mlOutputMap);
                }
                queryListener.onResponse(mlOutputMaps);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Prediction Failed:", e);
                queryListener.onFailure(e);
            }
        }, Math.max(inputMapSize, 1));
    }

    private boolean validateQueryFieldInQueryString(
        List<Map<String, String>> processInputMap,
        List<Map<String, String>> processOutputMap,
        String queryString
    ) {
        // the inputMap takes in model input as keys and query fields as value

        // Suppress errors thrown by JsonPath and instead return null if a path does not exist in a JSON blob.
        Configuration suppressExceptionConfiguration = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
        ReadContext jsonData = JsonPath.using(suppressExceptionConfiguration).parse(queryString);

        // check all values if exists in query
        for (Map<String, String> inputMap : processInputMap) {
            for (Map.Entry<String, String> entry : inputMap.entrySet()) {
                String queryField = entry.getValue();
                String pathData = jsonData.read(queryField);
                if (pathData == null) {
                    throw new IllegalArgumentException("cannot find mappings in query string for field: " + queryField);
                }
            }
        }
        if (queryTemplate == null) {
            for (Map<String, String> outputMap : processOutputMap) {
                for (Map.Entry<String, String> entry : outputMap.entrySet()) {
                    String queryField = entry.getKey();
                    String pathData = jsonData.read(queryField);
                    if (pathData == null) {
                        throw new IllegalArgumentException("cannot find mappings in query string for field: " + queryField);
                    }
                }
            }
        }
        return true;

    }

    private void processPredictions(
        String queryString,
        List<Map<String, String>> processInputMap,
        List<Map<String, String>> processOutputMap,
        int inputMapIndex,
        int inputMapSize,
        GroupedActionListener batchPredictionListener
    ) throws IOException {
        Map<String, String> modelParameters = new HashMap<>();
        Map<String, String> modelConfigs = new HashMap<>();

        if (inferenceProcessorAttributes.getModelConfigMaps() != null) {
            modelParameters.putAll(inferenceProcessorAttributes.getModelConfigMaps());
            modelConfigs.putAll(inferenceProcessorAttributes.getModelConfigMaps());
        }
        Map<String, String> inputMapping = new HashMap<>();

        // check input mapping
        // TODO what is the default input mapping?

        if (processInputMap != null) {
            // get {inputs: query.term.text} at 0
            inputMapping = processInputMap.get(inputMapIndex);
            Object newQuery = JsonPath.parse(queryString).read("$");
            for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
                // model field as key, query field name as value
                String modelInputFieldName = entry.getKey();
                String queryFieldName = entry.getValue();
                String queryFieldValue = JsonPath.parse(newQuery).read(queryFieldName);
                modelParameters.put(modelInputFieldName, queryFieldValue);
            }
        }

        Set<String> inputMapKeys = new HashSet<>(modelParameters.keySet());
        inputMapKeys.removeAll(modelConfigs.keySet());

        Map<String, String> inputMappings = new HashMap<>();
        for (String k : inputMapKeys) {
            inputMappings.put(k, modelParameters.get(k));
        }

        ActionRequest request = getMLModelInferenceRequest(
            xContentRegistry,
            modelParameters,
            modelConfigs,
            inputMappings,
            inferenceProcessorAttributes.getModelId(),
            functionName,
            modelInput
        );

        client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(MLTaskResponse mlTaskResponse) {
                MLOutput mlOutput = mlTaskResponse.getOutput();
                Map<Integer, MLOutput> mlOutputMap = new HashMap<>();
                mlOutputMap.put(inputMapIndex, mlOutput);
                batchPredictionListener.onResponse(mlOutputMap);
            }

            @Override
            public void onFailure(Exception e) {
                batchPredictionListener.onFailure(e);
            }
        });

    }

    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private final Client client;
        private final NamedXContentRegistry xContentRegistry;

        /**
         * Constructs a new instance of the Factory class.
         *
         * @param client           the Client instance to be used by the Factory
         * @param xContentRegistry the xContentRegistry instance to be used by the Factory
         */
        public Factory(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public MLInferenceSearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String processorTag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            String queryTemplate = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, QUERY_TEMPLATE);
            Map<String, Object> modelConfigInput = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
            List<Map<String, String>> inputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> outputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
            int maxPredictionTask = ConfigurationUtils
                .readIntProperty(TYPE, processorTag, config, MAX_PREDICTION_TASKS, DEFAULT_MAX_PREDICTION_TASKS);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);
            String functionName = ConfigurationUtils
                .readStringProperty(TYPE, processorTag, config, FUNCTION_NAME, FunctionName.REMOTE.name());
            String modelInput = ConfigurationUtils
                .readStringProperty(TYPE, processorTag, config, MODEL_INPUT, "{ \"parameters\": ${ml_inference.parameters} }");
            boolean defaultValue = !functionName.equalsIgnoreCase("remote");
            boolean fullResponsePath = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, FULL_RESPONSE_PATH, defaultValue);

            ignoreFailure = ConfigurationUtils
                .readBooleanProperty(TYPE, processorTag, config, ConfigurationUtils.IGNORE_FAILURE_KEY, false);
            // convert model config user input data structure to Map<String, String>
            Map<String, String> modelConfigMaps = null;
            if (modelConfigInput != null) {
                modelConfigMaps = StringUtils.getParameterMap(modelConfigInput);
            }
            // check if the number of prediction tasks exceeds max prediction tasks
            if (inputMaps != null && inputMaps.size() > maxPredictionTask) {
                throw new IllegalArgumentException(
                    "The number of prediction task setting in this process is "
                        + inputMaps.size()
                        + ". It exceeds the max_prediction_tasks of "
                        + maxPredictionTask
                        + ". Please reduce the size of input_map or increase max_prediction_tasks."
                );
            }
            if (inputMaps != null && outputMaps != null && outputMaps.size() != inputMaps.size()) {
                throw new IllegalArgumentException("The length of output_map and the length of input_map do no match.");
            }

            return new MLInferenceSearchRequestProcessor(
                modelId,
                queryTemplate,
                inputMaps,
                outputMaps,
                modelConfigMaps,
                maxPredictionTask,
                processorTag,
                description,
                ignoreMissing,
                functionName,
                fullResponsePath,
                ignoreFailure,
                modelInput,
                client,
                xContentRegistry
            );
        }
    }
}
