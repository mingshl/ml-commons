/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.ml.processor;

import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.processor.MLModelUtil.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.opensearch.action.ActionRequest;
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
 * MLInferenceIngestProcessor requires a model_id string to call model inferences
 * maps fields in document for model input, and maps model inference output to new document fields
 * this processor also handles dot path notation for nested object( map of array) by rewriting json path accordingly
 */
public class MLInferenceIngestProcessor extends AbstractProcessor implements ModelExecutor {

    private final MLModelUtil mlModelUtil;
    private final boolean ignoreMissing;
    private final ScriptService scriptService;
    private static Client client;
    public static final String TYPE = "ml_inference";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";

    protected MLInferenceIngestProcessor(
        String model_id,
        List<Map<String, String>> input_map,
        List<Map<String, String>> output_map,
        Map<String, String> model_config,
        String tag,
        String description,
        boolean ignoreMissing,
        ScriptService scriptService,
        Client client
    ) {
        super(tag, description);
        this.mlModelUtil = new MLModelUtil(model_id, input_map, output_map, model_config);
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

        List<Map<String, String>> process_input = mlModelUtil.getInput_map();
        List<Map<String, String>> process_output = mlModelUtil.getOutput_map();
        int i = 0;
        int round = (process_input != null) ? process_input.size() : 0;

        process_predictions(ingestDocument, handler, process_input, process_output, i, round);

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
    private void process_predictions(
        IngestDocument ingestDocument,
        BiConsumer<IngestDocument, Exception> handler,
        List<Map<String, String>> process_input,
        List<Map<String, String>> process_output,
        int i,
        int round
    ) {
        if (i >= round && i != 0) {
            handler.accept(ingestDocument, null);
        } else {

            Map<String, String> modelParameters = new HashMap<>();

            if (mlModelUtil.getModel_config() != null) {
                modelParameters.putAll(mlModelUtil.getModel_config());
            }
            // when no input mapping is provided, default to read all fields from documents as model input
            if (round == 0) {
                Set<String> documentFields = ingestDocument.getSourceAndMetadata().keySet();
                for (String field : documentFields) {
                    getMappedModelInputFromDocuments(ingestDocument, modelParameters, field, field);
                }

            } else {
                Map<String, String> inputMapping = process_input.get(i);
                for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
                    String originalFieldName = entry.getKey();
                    String ModelInputFieldName = entry.getValue();
                    getMappedModelInputFromDocuments(ingestDocument, modelParameters, originalFieldName, ModelInputFieldName);
                }
            }

            ActionRequest request = getRemoteModelInferenceRequest(modelParameters, mlModelUtil.getModel_id());

            client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {

                @Override
                public void onResponse(MLTaskResponse mlTaskResponse) {
                    ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                    if (process_output == null || process_output.isEmpty()) {
                        appendFieldValue(modelTensorOutput, null, DEFAULT_OUTPUT_FIELD_NAME, ingestDocument);
                    } else {
                        Map<String, String> outputMapping = process_output.get(i);

                        for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
                            String originalModelOutputFieldName = entry.getKey(); // response
                            String newModelOutputFieldName = entry.getValue(); // inference_result
                            appendFieldValue(modelTensorOutput, originalModelOutputFieldName, newModelOutputFieldName, ingestDocument);
                        }
                    }
                    process_predictions(ingestDocument, handler, process_input, process_output, i + 1, round);
                }

                @Override
                public void onFailure(Exception e) {
                    handler.accept(null, e);
                    return;
                }
            });
        }

    }

    private void getMappedModelInputFromDocuments(
        IngestDocument ingestDocument,
        Map<String, String> modelParameters,
        String originalFieldName,
        String ModelInputFieldName
    ) {
        String originalFieldPath = getFieldPath(ingestDocument, originalFieldName);
        if (originalFieldPath != null) {
            Object originalFieldValue = ingestDocument.getFieldValue(originalFieldPath, Object.class);
            String originalFieldValueAsString = toString(originalFieldValue);
            updateModelParameters(ModelInputFieldName, originalFieldValueAsString, modelParameters);
        }
        // check for nested array
        else {
            if (originalFieldName.contains(".")) {

                Map<String, Object> sourceObject = ingestDocument.getSourceAndMetadata();
                Configuration suppressExceptionConfiguration = Configuration
                    .builder()
                    .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL, Option.ALWAYS_RETURN_LIST)
                    .build();
                String rewriteDotPathForNestedObject = findDotPathForNestedObject(sourceObject, originalFieldName);
                ArrayList<Object> fieldValueList = JsonPath
                    .using(suppressExceptionConfiguration)
                    .parse(sourceObject)
                    .read(rewriteDotPathForNestedObject);

                String originalFieldValueAsString = toString(fieldValueList);
                updateModelParameters(ModelInputFieldName, originalFieldValueAsString, modelParameters);
            }
        }
    }

    /**
     * support multiple document fields to map to the same model input fields,
     * check if the key existed, then append to existed value to be a list.
     */
    private void updateModelParameters(String ModelInputFieldName, String originalFieldValueAsString, Map<String, String> modelParameters) {

        if (modelParameters.containsKey(ModelInputFieldName)) {
            Object existingValue = modelParameters.get(ModelInputFieldName);
            List<Object> updatedList;

            if (!(existingValue instanceof List)) {
                updatedList = new ArrayList<>();
                updatedList.add(existingValue);
            } else {
                updatedList = (List<Object>) existingValue;
            }

            updatedList.add(originalFieldValueAsString);

            modelParameters.put(ModelInputFieldName, toString(updatedList));
        } else {
            modelParameters.put(ModelInputFieldName, originalFieldValueAsString);
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
        String newModelOutputFieldName,
        IngestDocument ingestDocument
    ) {
        Object modelOutputValue = null;

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
            throw new RuntimeException("model inference output cannot be null");
        }

        List<String> dotPathsInArray = writeNewDotPathForNestedObject(ingestDocument.getSourceAndMetadata(), newModelOutputFieldName);

        if (dotPathsInArray.size() == 1) {
            ValueSource ingestValue = ValueSource.wrap(modelOutputValue, scriptService);
            TemplateScript.Factory ingestField = ConfigurationUtils
                .compileTemplate(TYPE, tag, newModelOutputFieldName, newModelOutputFieldName, scriptService);
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
                        + newModelOutputFieldName
                        + " is an array in size of "
                        + modelOutputValueArray.size()
                        + " but the document field array from field "
                        + newModelOutputFieldName
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
            String model_id = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            Map<String, Object> model_config_input = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
            List<Map<String, String>> input_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> output_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);

            // convert model config user input data structure to Map<String, String>
            Map<String, String> model_config = null;
            if (model_config_input != null) {
                model_config = new HashMap<>();
                for (String key : model_config_input.keySet()) {
                    Object value = model_config_input.get(key);
                    if (value instanceof String) {
                        model_config.put(key, (String) value);
                    } else {
                        model_config.put(key, gson.toJson(value));
                    }
                }
            }

            return new MLInferenceIngestProcessor(
                model_id,
                input_map,
                output_map,
                model_config,
                processorTag,
                description,
                ignoreMissing,
                scriptService,
                client
            );
        }
    }

}
