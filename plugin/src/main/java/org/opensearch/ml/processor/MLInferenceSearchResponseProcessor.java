package org.opensearch.ml.processor;

import static org.opensearch.ml.processor.InferenceProcessorAttributes.INPUT_MAP;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MAX_PREDICTION_TASKS;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_CONFIG;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_ID;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.OUTPUT_MAP;
import static org.opensearch.ml.processor.MLInferenceIngestProcessor.IGNORE_MISSING;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Client;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import lombok.SneakyThrows;

public class MLInferenceSearchResponseProcessor extends AbstractProcessor implements SearchResponseProcessor, ModelExecutor {

    public static final String $_SOURCE_PATH = "$.[*]._source.";
    private final InferenceProcessorAttributes inferenceProcessorAttributes;
    private final boolean ignoreMissing;
    private final boolean ignoreFailure;
    private static Client client;
    public static final String TYPE = "ml_inference";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";
    // At default, ml inference processor allows maximum 10 prediction tasks running in parallel
    // it can be overwritten using max_prediction_tasks when creating processor
    public static final int DEFAULT_MAX_PREDICTION_TASKS = 10;

    protected MLInferenceSearchResponseProcessor(
        String modelId,
        List<Map<String, String>> input_map,
        List<Map<String, String>> output_map,
        Map<String, String> model_config,
        String tag,
        String description,
        boolean ignoreMissing,
        boolean ignoreFailure,
        Client client
    ) {
        super(tag, description, ignoreFailure);
        this.inferenceProcessorAttributes = new InferenceProcessorAttributes(
            modelId,
            input_map,
            output_map,
            model_config,
            DEFAULT_MAX_PREDICTION_TASKS
        );
        this.ignoreMissing = ignoreMissing;
        this.ignoreFailure = ignoreFailure;
        this.client = client;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        boolean foundField = false;

        List<Map<String, String>> processInputMap = inferenceProcessorAttributes.getInputMaps();
        List<Map<String, String>> processOutputMap = inferenceProcessorAttributes.getOutputMaps();
        int inputMapSize = (processInputMap != null) ? processInputMap.size() : 0;

        SearchHit[] hits = response.getHits().getHits();
        // Convert SearchHit[] to JSON
        String hitsJson = new ObjectMapper().writeValueAsString(hits);

        GroupedActionListener<Void> batchPredictionListener = new GroupedActionListener<>(new ActionListener<Collection<Void>>() {
            @Override
            public void onResponse(Collection<Void> voids) {}

            @Override
            public void onFailure(Exception e) {
                if (ignoreFailure) {

                } else {

                }
            }
        }, Math.max(inputMapSize, 1));

        for (int inputMapIndex = 0; inputMapIndex < Math.max(inputMapSize, 1); inputMapIndex++) {
            try {
                processPredictions(hits, hitsJson, batchPredictionListener, processInputMap, processOutputMap, inputMapIndex, inputMapSize);
            } catch (Exception e) {
                batchPredictionListener.onFailure(e);
            }
        }
        return response;

        // SearchHit[] hits = response.getHits().getHits();
        //
        // // step 1 get field for model input
        // for (SearchHit hit : hits) {
        // Map<String, DocumentField> fields = hit.getFields();
        // if (fields.containsKey(oldField)) {
        // foundField = true;
        // DocumentField old = hit.removeDocumentField(oldField);
        // DocumentField newDocField = new DocumentField(newField, old.getValues());
        // hit.setDocumentField(newField, newDocField);
        // }
        //
        // // step 2 send model input for model prediction
        //
        //
        // // step 3 append prediction outcomes to hits
        // if (!foundField && !ignoreMissing) {
        // throw new IllegalArgumentException("Document with id " + hit.getId() + " is missing field " + oldField);
        // }

        // return null;
    }

    private void processPredictions(
        SearchHit[] hits,
        String hitsJson,
        GroupedActionListener<Void> batchPredictionListener,
        List<Map<String, String>> processInputMap,
        List<Map<String, String>> processOutputMap,
        int inputMapIndex,
        int inputMapSize
    ) {
        // SearchHit[] hits = response.getHits().getHits();
        // for (SearchHit hit : hits) {
        // Map<String, DocumentField> fields = hit.getSourceAsString();}
        Map<String, String> modelParameters = new HashMap<>();
        if (inferenceProcessorAttributes.getModelConfigMaps() != null) {
            modelParameters.putAll(inferenceProcessorAttributes.getModelConfigMaps());
        }
        // when no input mapping is provided, default to read all fields from documents as model input
        if (inputMapSize == 0) {
            // TODO to handle multiple list of input map.

            // List modelInputFieldList = new ArrayList();
            // SearchHit[] hits = response.getHits().getHits();
            // for (SearchHit hit : hits) {
            // Map<String, DocumentField> fields = hit.getFields();
            // if (fields.containsKey()) {
            //
            // }
            // }
            // Set<String> documentFields = ingestDocument.getSourceAndMetadata().keySet();
            // for (String field : documentFields) {
            // getMappedModelInputFromDocuments(ingestDocument, modelParameters, field, field);
            // }

        } else {
            Map<String, String> inputMapping = processInputMap.get(inputMapSize);
            for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
                String modelInputFieldName = entry.getKey();// inputs
                String documentFieldName = entry.getValue();

                String jsonPath = $_SOURCE_PATH + documentFieldName;
                Object documentValue = JsonPath.read(hitsJson, jsonPath);
                modelParameters.put(modelInputFieldName, toString(documentValue));
            }

        }
        ActionRequest request = getRemoteModelInferenceRequest(modelParameters, inferenceProcessorAttributes.getModelId());

        client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {

            @SneakyThrows
            @Override
            public void onResponse(MLTaskResponse mlTaskResponse) {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();

                if (processOutputMap == null || processOutputMap.isEmpty()) {
                    // ToDo handle not output list
                    //
                    // ArrayList<Object> modelTensorOutputList = new ArrayList<>();
                    //
                    // modelTensorOutputList.add(modelTensorOutput);
                    // DocumentField newDocField = new DocumentField(DEFAULT_OUTPUT_FIELD_NAME, modelTensorOutputList);
                    // for (SearchHit hit : hits) {
                    // hit.setDocumentField(DEFAULT_OUTPUT_FIELD_NAME, newDocField);
                    // }
                } else {
                    Map<String, String> outputMapping = processOutputMap.get(inputMapIndex);

                    // check having the same field name in the hits
                    for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
                        // document field as key, model field as value
                        String newDocumentFieldName = entry.getKey(); // response
                        String modelOutputFieldName = entry.getValue(); // inference_result
                        appendFieldValue(modelTensorOutput, modelOutputFieldName, newDocumentFieldName, hits);
                    }
                }

            }

            @Override
            public void onFailure(Exception e) {
                batchPredictionListener.onFailure(e);
            }
        });

    }

    private void appendFieldValue(
        ModelTensorOutput modelTensorOutput,
        String modelOutputFieldName,
        String newDocumentFieldName,
        SearchHit[] hits
    ) throws IOException {
        Object modelOutputValue = null;

        if (modelTensorOutput.getMlModelOutputs() != null && modelTensorOutput.getMlModelOutputs().size() > 0) {

            modelOutputValue = getModelOutputValue(modelTensorOutput, modelOutputFieldName, ignoreMissing);

            // if output is not a list
            if (!(modelOutputValue instanceof List)) {
                // throw new IllegalArgumentException("Model output is not an array, cannot assign to array in documents.");
            } else {
                List<?> modelOutputValueArray = (List<?>) modelOutputValue;
                if (hits.length != modelOutputValueArray.size()) {
                    throw new RuntimeException(
                        "the prediction field: "
                            + modelOutputFieldName
                            + " is an array in size of "
                            + modelOutputValueArray.size()
                            + " but the document field array from hits "
                            + newDocumentFieldName
                            + " is in size of "
                            + hits.length
                    );
                }
                int modelOutputValueIndex = 0;
                for (SearchHit hit : hits) {

                    // List<Object> modelTensorOutputDocument = new ArrayList<>();
                    // modelTensorOutputDocument.add(modelTensorOutputList.get(i));
                    // TODO DEFAULT_OUTPUT_FIELD_NAME
                    if (newDocumentFieldName == null) {
                        newDocumentFieldName = DEFAULT_OUTPUT_FIELD_NAME;
                    }
                    DocumentField newDocField = new DocumentField(
                        newDocumentFieldName,
                        List.of(modelOutputValueArray.get(modelOutputValueIndex))
                    );
                    hit.setDocumentField(newDocumentFieldName, newDocField);

                    if (hit.hasSource()) {
                        BytesReference sourceRef = hit.getSourceRef();
                        Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper
                            .convertToMap(sourceRef, false, (MediaType) null);

                        Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                        if (!sourceAsMap.containsKey(newDocumentFieldName)) {
                            sourceAsMap.put(DEFAULT_OUTPUT_FIELD_NAME, modelOutputValueArray.get(modelOutputValueIndex));

                            XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());

                            builder.map(sourceAsMap);

                            hit.sourceRef(BytesReference.bytes(builder));
                        }
                    }
                }

                // List<String> dotPathsInArray = writeNewDotPathForNestedObject(ingestDocument.getSourceAndMetadata(),
                // newDocumentFieldName);
                //
                // if (dotPathsInArray.size() == 1) {
                // ValueSource ingestValue = ValueSource.wrap(modelOutputValue, scriptService);
                // TemplateScript.Factory ingestField = ConfigurationUtils
                // .compileTemplate(TYPE, tag, newDocumentFieldName, newDocumentFieldName, scriptService);
                // ingestDocument.setFieldValue(ingestField, ingestValue, ignoreMissing);
                // }
                // else {
                // if (!(modelOutputValue instanceof List)) {
                // throw new IllegalArgumentException("Model output is not an array, cannot assign to array in documents.");
                // }
                // List<?> modelOutputValueArray = (List<?>) modelOutputValue;
                // // check length of the prediction array to be the same of the document array
                // if (dotPathsInArray.size() != modelOutputValueArray.size()) {
                // throw new RuntimeException(
                // "the prediction field: "
                // + modelOutputFieldName
                // + " is an array in size of "
                // + modelOutputValueArray.size()
                // + " but the document field array from field "
                // + newDocumentFieldName
                // + " is in size of "
                // + dotPathsInArray.size()
                // );
                // }
                // // Iterate over dotPathInArray
                // for (int i = 0; i < dotPathsInArray.size(); i++) {
                // String dotPathInArray = dotPathsInArray.get(i);
                // Object modelOutputValueInArray = modelOutputValueArray.get(i);
                // ValueSource ingestValue = ValueSource.wrap(modelOutputValueInArray, scriptService);
                // TemplateScript.Factory ingestField = ConfigurationUtils
                // .compileTemplate(TYPE, tag, dotPathInArray, dotPathInArray, scriptService);
                // ingestDocument.setFieldValue(ingestField, ingestValue, ignoreMissing);
                // }
                // }
                // } else {
                // throw new RuntimeException("model inference output cannot be null");
                // }
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * This is a Factory that creates the MLInferenceSearchResponseProcessor
     */
    public static final class Factory implements Processor.Factory<SearchResponseProcessor> {

        private final Client client;

        public Factory(Client client) {
            this.client = client;
        }

        /**
         * Creates a new instance of the MLInferenceSearchProcessor.
         *
         * @param processorFactories     a map of registered processor factories
         * @param processorTag a unique tag for the processor
         * @param description  a description of the processor
         * @param config       a map of configuration properties for the processor
         * @return a new instance of the MLInferenceIngestProcessor
         * @throws Exception if there is an error creating the processor
         */

        @Override
        public MLInferenceSearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String processorTag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            Map<String, Object> modelConfigInput = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
            List<Map<String, String>> inputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> outputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);
            int maxPredictionTask = ConfigurationUtils
                .readIntProperty(TYPE, processorTag, config, MAX_PREDICTION_TASKS, DEFAULT_MAX_PREDICTION_TASKS);

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

            return new MLInferenceSearchResponseProcessor(
                modelId,
                inputMaps,
                outputMaps,
                modelConfigMaps,
                processorTag,
                description,
                ignoreMissing,
                ignoreFailure,
                client
            );
        }
    }
}
