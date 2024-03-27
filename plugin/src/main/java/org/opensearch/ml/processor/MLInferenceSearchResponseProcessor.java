/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.processor;

import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import static org.opensearch.ml.processor.InferenceProcessorAttributes.INPUT_MAP;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MAX_PREDICTION_TASKS;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_CONFIG;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_ID;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.OUTPUT_MAP;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
public class MLInferenceSearchResponseProcessor extends AbstractProcessor implements SearchResponseProcessor, ModelExecutor {

    private final NamedXContentRegistry xContentRegistry;
    private static final Logger logger = LogManager.getLogger(MLInferenceSearchResponseProcessor.class);
    private final InferenceProcessorAttributes inferenceProcessorAttributes;
    private final boolean ignoreMissing;
    private final String functionName;
    private String queryTemplate;
    private final boolean fullResponsePath;
    private final boolean ignoreFailure;
    private final String modelInput;
    private static Client client;
    public static final String TYPE = "ml_inference";
    // allow to ignore a field from mapping is not present in the query, and when the output field is not found in the
    // prediction outcomes, return the whole prediction outcome by skipping filtering
    public static final String IGNORE_MISSING = "ignore_missing";
    public static final String QUERY_TEMPLATE = "query_template";
    public static final String FUNCTION_NAME = "function_name";
    public static final String FULL_RESPONSE_PATH = "full_response_path";
    public static final String MODEL_INPUT = "model_input";
    public static final String DEFAULT_MODEl_INPUT = "{ \"parameters\": ${ml_inference.parameters} }";
    // At default, ml inference processor allows maximum 10 prediction tasks running in parallel
    // it can be overwritten using max_prediction_tasks when creating processor
    public static final int DEFAULT_MAX_PREDICTION_TASKS = 10;

    protected MLInferenceSearchResponseProcessor(
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

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
    return response;
    }



    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    /**
     * Returns the type of the processor.
     *
     * @return the type of the processor as a string
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * A factory class for creating instances of the MLInferenceSearchResponseProcessor.
     * This class implements the Processor.Factory interface for creating SearchResponseProcessor instances.
     */
    public static class Factory implements Processor.Factory<SearchResponseProcessor> {
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

        /**
         * Creates a new instance of the MLInferenceSearchResponseProcessor.
         *
         * @param processorFactories a map of processor factories
         * @param processorTag       the tag of the processor
         * @param description        the description of the processor
         * @param ignoreFailure      a flag indicating whether to ignore failures or not
         * @param config             the configuration map for the processor
         * @param pipelineContext    the pipeline context
         * @return a new instance of the MLInferenceSearchResponseProcessor
         */
        @Override
        public MLInferenceSearchResponseProcessor create(
                Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
                String processorTag,
                String description,
                boolean ignoreFailure,
                Map<String, Object> config,
                PipelineContext pipelineContext
        ) {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            String queryTemplate = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, QUERY_TEMPLATE);
            Map<String, Object> modelConfigInput = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);

            List<Map<String, String>> inputMaps = ConfigurationUtils.readList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> outputMaps = ConfigurationUtils.readList(TYPE, processorTag, config, OUTPUT_MAP);
            int maxPredictionTask = ConfigurationUtils
                    .readIntProperty(TYPE, processorTag, config, MAX_PREDICTION_TASKS, DEFAULT_MAX_PREDICTION_TASKS);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);
            String functionName = ConfigurationUtils
                    .readStringProperty(TYPE, processorTag, config, FUNCTION_NAME, FunctionName.REMOTE.name());
            String modelInput = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, MODEL_INPUT);

            // if model input is not provided for remote models, use default value
            if (functionName.equalsIgnoreCase("remote")) {
                modelInput = (modelInput != null) ? modelInput : DEFAULT_MODEl_INPUT;
            } else if (modelInput == null) {
                // if model input is not provided for local models, throw exception since it is mandatory here
                throw new IllegalArgumentException("Please provide model input when using a local model in ML Inference Processor");
            }
            boolean defaultFullResponsePath = !functionName.equalsIgnoreCase(FunctionName.REMOTE.name());
            boolean fullResponsePath = ConfigurationUtils
                    .readBooleanProperty(TYPE, processorTag, config, FULL_RESPONSE_PATH, defaultFullResponsePath);

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

            return new MLInferenceSearchResponseProcessor(
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
