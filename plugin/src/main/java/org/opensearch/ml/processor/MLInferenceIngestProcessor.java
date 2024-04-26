/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.ml.processor;

import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.processor.InferenceProcessorAttributes.*;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.ValueSource;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

/**
 * MLInferenceIngestProcessor requires a modelId string to call model inferences
 * maps fields in document for model input, and maps model inference output to new document fields
 * this processor also handles dot path notation for nested object( map of array) by rewriting json path accordingly
 */
public class MLInferenceIngestProcessor extends AbstractProcessor implements ModelExecutor {

    private final InferenceProcessorAttributes inferenceProcessorAttributes;
    private final boolean ignoreMissing;
    private final ScriptService scriptService;
    private static Client client;
    public static final String TYPE = "ml_inference";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";

    public static final String IGNORE_MISSING = "ignore_missing";

    public static final int DEFAULT_MAX_PREDICTION_TASKS = 10;

    private Configuration suppressExceptionConfiguration = Configuration
        .builder()
        .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL, Option.ALWAYS_RETURN_LIST)
        .build();

    protected MLInferenceIngestProcessor(
        String modelId,
        List<Map<String, String>> inputMaps,
        List<Map<String, String>> outputMaps,
        Map<String, String> modelConfigMaps,
        int maxPredictionTask,
        String tag,
        String description,
        boolean ignoreMissing,
        ScriptService scriptService,
        Client client
    ) {
        super(tag, description);
        this.inferenceProcessorAttributes = new InferenceProcessorAttributes(
            modelId,
            inputMaps,
            outputMaps,
            modelConfigMaps,
            maxPredictionTask
        );
        this.ignoreMissing = ignoreMissing;
        this.scriptService = scriptService;
        this.client = client;
    }

    /**
     * overwrite in this execute method,
     * when batch inference is available,
     * to support async multiple predictions.
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {

        List<Map<String, String>> processInputMap = inferenceProcessorAttributes.getInputMaps();
        List<Map<String, String>> processOutputMap = inferenceProcessorAttributes.getOutputMaps();
        int num_of_prediction_tasks = (processInputMap != null) ? processInputMap.size() : 0;

        GroupedActionListener<Void> batchPredictionListener = new GroupedActionListener<>(new ActionListener<Collection<Void>>() {
            @Override
            public void onResponse(Collection<Void> voids) {
                handler.accept(ingestDocument, null);
            }

            @Override
            public void onFailure(Exception e) {
                handler.accept(null, e);
            }
        }, Math.max(num_of_prediction_tasks, 1));

        for (int i = 0; i < Math.max(num_of_prediction_tasks, 1); i++) {
            processPredictions(ingestDocument, batchPredictionListener, processInputMap, processOutputMap, 0, num_of_prediction_tasks);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        return ingestDocument;
    }

    /**
     * process predictions for one model for multiple rounds of predictions
     * ingest documents after prediction rounds are completed,
     * when no input mappings provided, default to add all fields to model input fields,
     * when no output mapping provided, default to output as
     * "inference_results" field (the same format as predict API)
     */
    private void processPredictions(
        IngestDocument ingestDocument,
        GroupedActionListener<Void> batchPredictionListener,
        List<Map<String, String>> processInputMap,
        List<Map<String, String>> processOutputMap,
        int i,
        int num_of_prediction_tasks
    ) {
        Map<String, String> modelParameters = new HashMap<>();
        if (inferenceProcessorAttributes.getModelConfigMaps() != null) {
            modelParameters.putAll(inferenceProcessorAttributes.getModelConfigMaps());
        }
        // when no input mapping is provided, default to read all fields from documents as model input
        if (num_of_prediction_tasks == 0) {
            Set<String> documentFields = ingestDocument.getSourceAndMetadata().keySet();
            for (String field : documentFields) {
                getMappedModelInputFromDocuments(ingestDocument, modelParameters, field, field);
            }

        } else {
            Map<String, String> inputMapping = processInputMap.get(i);
            for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
                // model field as key, document field as value
                String originalFieldName = entry.getValue();
                String modelInputFieldName = entry.getKey();
                getMappedModelInputFromDocuments(ingestDocument, modelParameters, originalFieldName, modelInputFieldName);
            }
        }

        ActionRequest request = getRemoteModelInferenceRequest(modelParameters, inferenceProcessorAttributes.getModelId());

        client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(MLTaskResponse mlTaskResponse) {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                if (processOutputMap == null || processOutputMap.isEmpty()) {
                    appendFieldValue(modelTensorOutput, null, DEFAULT_OUTPUT_FIELD_NAME, ingestDocument);
                } else {
                    Map<String, String> outputMapping = processOutputMap.get(i);

                    for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
                        String originalModelOutputFieldName = entry.getKey();
                        String newDocumentFieldName = entry.getValue();
                        appendFieldValue(modelTensorOutput, originalModelOutputFieldName, newDocumentFieldName, ingestDocument);
                    }
                }
                batchPredictionListener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                batchPredictionListener.onFailure(e);
            }
        });

    }

    private void getMappedModelInputFromDocuments(
        IngestDocument ingestDocument,
        Map<String, String> modelParameters,
        String originalFieldName,
        String modelInputFieldName
    ) {
        String originalFieldPath = getFieldPath(ingestDocument, originalFieldName);
        if (originalFieldPath != null) {
            Object originalFieldValue = ingestDocument.getFieldValue(originalFieldPath, Object.class);
            String originalFieldValueAsString = toString(originalFieldValue);
            updateModelParameters(modelInputFieldName, originalFieldValueAsString, modelParameters);
        }
        // check for nested array
        else {
            if (originalFieldName.contains(".")) {

                Map<String, Object> sourceObject = ingestDocument.getSourceAndMetadata();
                String rewriteDotPathForNestedObject = findDotPathForNestedObject(sourceObject, originalFieldName);
                ArrayList<Object> fieldValueList = JsonPath
                    .using(suppressExceptionConfiguration)
                    .parse(sourceObject)
                    .read(rewriteDotPathForNestedObject);

                String originalFieldValueAsString = toString(fieldValueList);
                updateModelParameters(modelInputFieldName, originalFieldValueAsString, modelParameters);
            }
        }
    }

    /**
     * support multiple document fields to map to the same model input fields,
     * check if the key existed, then append to existed value to be a list.
     */
    private void updateModelParameters(String modelInputFieldName, String originalFieldValueAsString, Map<String, String> modelParameters) {

        if (modelParameters.containsKey(modelInputFieldName)) {
            Object existingValue = modelParameters.get(modelInputFieldName);
            List<Object> updatedList = (List<Object>) existingValue;

            updatedList.add(originalFieldValueAsString);

            modelParameters.put(modelInputFieldName, toString(updatedList));
        } else {
            modelParameters.put(modelInputFieldName, originalFieldValueAsString);
        }

    }

    private String getFieldPath(IngestDocument ingestDocument, String originalFieldName) {
        final boolean fieldPathIsNullOrEmpty = Strings.isNullOrEmpty(originalFieldName);

        if (fieldPathIsNullOrEmpty) {
            return null;
        }
        final boolean hasFieldPath = ingestDocument.hasField(originalFieldName, true);
        if (!hasFieldPath) {
            return null;
        }
        return originalFieldName;
    }

    /**
     * appendFieldValue to the ingestDocument without changing the source
     */

    private void appendFieldValue(
        ModelTensorOutput modelTensorOutput,
        String originalModelOutputFieldName,
        String newDocumentFieldName,
        IngestDocument ingestDocument
    ) {
        Object modelOutputValue = null;
        if (modelTensorOutput.getMlModelOutputs() != null) {
            Map<String, ?> modelTensorOutputMap = modelTensorOutput.getMlModelOutputs().get(0).getMlModelTensors().get(0).getDataAsMap();

            if (modelTensorOutputMap != null) {
                try {
                    modelOutputValue = getModelOutputField(modelTensorOutputMap, originalModelOutputFieldName, ignoreMissing);
                } catch (IOException e) {
                    if (!ignoreMissing) {
                        throw new IllegalArgumentException(
                            "model inference output can not find field name: " + originalModelOutputFieldName,
                            e
                        );
                    }
                }
            } else {
                // further check if there is array in getData
                ArrayList tensorArray = new ArrayList<>();
                for (int i = 0; i < modelTensorOutput.getMlModelOutputs().get(0).getMlModelTensors().size(); i++) {

                    Object tensorData = modelTensorOutput.getMlModelOutputs().get(0).getMlModelTensors().get(i).getData();
                    tensorArray.add(Arrays.asList(tensorData));
                }
                modelOutputValue = tensorArray;
                if (modelOutputValue == null) {
                    throw new RuntimeException("model inference output cannot be null");
                }
            }

            List<String> dotPathsInArray = writeNewDotPathForNestedObject(ingestDocument.getSourceAndMetadata(), newDocumentFieldName);

            if (dotPathsInArray.size() == 1) {
                ValueSource ingestValue = ValueSource.wrap(modelOutputValue, scriptService);
                TemplateScript.Factory ingestField = ConfigurationUtils
                    .compileTemplate(TYPE, tag, newDocumentFieldName, newDocumentFieldName, scriptService);
                ingestDocument.appendFieldValue(ingestField, ingestValue, false);
            } else {
                ArrayList<?> modelOutputValueArray;
                if (modelOutputValue instanceof List) {
                    List<?> modelOutputList = (List<?>) modelOutputValue;
                    modelOutputValueArray = new ArrayList<>(modelOutputList);
                } else if (modelOutputValue instanceof ArrayList) {
                    modelOutputValueArray = (ArrayList<?>) modelOutputValue;
                } else {
                    throw new IllegalArgumentException("model output is not an array, cannot assign to array in documents.");
                }

                // check length of the prediction array to be the same of the document array
                if (dotPathsInArray.size() != modelOutputValueArray.size()) {
                    throw new RuntimeException(
                        "the prediction field: "
                            + originalModelOutputFieldName
                            + " is an array in size of "
                            + modelOutputValueArray.size()
                            + " but the document field array from field "
                            + newDocumentFieldName
                            + " is in size of "
                            + dotPathsInArray.size()
                    );
                }
                // Iterate over dotPathInArray
                for (int i = 0; i < dotPathsInArray.size(); i++) {
                    String dotPathInArray = dotPathsInArray.get(i);
                    Object modelOutputValueInArray = modelOutputValueArray.get(i);
                    ValueSource ingestValue = ValueSource.wrap(modelOutputValueInArray, scriptService);
                    TemplateScript.Factory ingestField = ConfigurationUtils
                        .compileTemplate(TYPE, tag, dotPathInArray, dotPathInArray, scriptService);
                    ingestDocument.appendFieldValue(ingestField, ingestValue, false);
                }
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        private final ScriptService scriptService;
        private final Client client;

        public Factory(ScriptService scriptService, Client client) {
            this.scriptService = scriptService;
            this.client = client;
        }

        @Override
        public MLInferenceIngestProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config

        ) throws Exception {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            Map<String, Object> modelConfigInput = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
            List<Map<String, String>> inputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> outputMaps = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
            int maxPredictionTask = ConfigurationUtils
                .readIntProperty(TYPE, processorTag, config, MAX_PREDICTION_TASKS, DEFAULT_MAX_PREDICTION_TASKS);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);

            // convert model config user input data structure to Map<String, String>
            Map<String, String> modelConfigMaps = null;
            if (modelConfigInput != null) {
                modelConfigMaps = new HashMap<>();
                for (String key : modelConfigInput.keySet()) {
                    Object value = modelConfigInput.get(key);
                    if (value instanceof String) {
                        modelConfigMaps.put(key, (String) value);
                    } else {
                        modelConfigMaps.put(key, gson.toJson(value));
                    }
                }
            }

            // check if the number of prediction tasks exceeds max prediction tasks

            if (inputMaps != null && inputMaps.size() > maxPredictionTask) {
                throw new IllegalArgumentException(
                    "The number of prediction task setting in this process is "
                        + inputMaps.size()
                        + ". It exceeds the max_prediction_tasks of "
                        + maxPredictionTask
                        + ". Please reduce the length of input_map or increase max_prediction_tasks."
                );
            }
            if (inputMaps != null && outputMaps != null && outputMaps.size() != inputMaps.size()) {
                throw new IllegalArgumentException("The length of output_map and the length of input_map do no match.");
            }

            return new MLInferenceIngestProcessor(
                modelId,
                inputMaps,
                outputMaps,
                modelConfigMaps,
                maxPredictionTask,
                processorTag,
                description,
                ignoreMissing,
                scriptService,
                client
            );
        }
    }

}
