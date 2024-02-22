package org.opensearch.ml.processor;

import static org.opensearch.ml.processor.MLModelUtil.*;

import java.util.*;
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
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.model.MLModelCacheHelper;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

public class MLInferenceProcessor extends AbstractProcessor implements ModelExecutor {

    private final MLModelUtil mlModelUtil;
    private final boolean ignoreMissing;
    private final ScriptService scriptService;
    private static Client client;
    private final MLModelCacheHelper modelCacheHelper;
    public static final String TYPE = "ml_inference";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";

    protected MLInferenceProcessor(
        String model_id,
        List<Map<String, String>> input_map,
        List<Map<String, String>> output_map,
        Map<String, String> model_config,
        String tag,
        String description,
        boolean ignoreMissing,
        ScriptService scriptService,
        Client client,
        MLModelCacheHelper modelCacheHelper
    ) {
        super(tag, description);
        this.mlModelUtil = new MLModelUtil(model_id, input_map, output_map, model_config);
        this.ignoreMissing = ignoreMissing;
        this.scriptService = scriptService;
        this.client = client;
        this.modelCacheHelper = modelCacheHelper;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {

        List<Map<String, String>> process_input = mlModelUtil.getInput_map();
        List<Map<String, String>> process_output = mlModelUtil.getOutput_map();
        // TODO handle process_input is null
        int i = 0;

        int round = (process_input != null) ? process_input.size() : 0;

        process_predictions(ingestDocument, handler, process_input, process_output, i, round);

    }

    /**
     * process predictions for one model for multiple rounds of predictions
     * ingest documents after prediction rounds are completed
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
            // TODO handle output mapping is null, default logic
            // when no input mapping is provided, default to read all fields from documents as model input
            if (round == 0) {
                Set<String> documentFields = ingestDocument.getSourceAndMetadata().keySet();
                for (String field : documentFields) {
                    getMappedModelInputFromDocuments(ingestDocument, modelParameters, field, field);
                }

            } else {
                Map<String, String> inputMapping = process_input.get(i);

                // Step 1 Mapping input fields.
                // conduct field mapping from the original documents to known fields for model input
                // process input
                for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
                    String originalFieldName = entry.getKey();
                    String ModelInputFieldName = entry.getValue();

                    getMappedModelInputFromDocuments(ingestDocument, modelParameters, originalFieldName, ModelInputFieldName);
                }
            }

            // Step 2 //process InputFields and make predictions

            ActionRequest request = getRemoteModelInferenceResult(modelParameters, mlModelUtil.getModel_id());

            client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {

                @Override
                public void onResponse(MLTaskResponse mlTaskResponse) {
                    ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();

                    // when no output mapping provided, default to output as "inference_results"

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
        if (originalFieldPath != null ) {
            Object originalFieldValue = ingestDocument.getFieldValue(originalFieldPath, Object.class);
            String originalFieldValueAsString = getModelInputFieldValue(originalFieldValue);
            modelParameters.put(ModelInputFieldName, originalFieldValueAsString);
        }
    }

    private String getFieldPath(IngestDocument ingestDocument, String originalFieldName) {
        TemplateScript.Factory originalField = ConfigurationUtils.compileTemplate(TYPE, tag, "field", originalFieldName, scriptService);
        String originalFieldPath = ingestDocument.renderTemplate(originalField);
        final boolean fieldPathIsNullOrEmpty = Strings.isNullOrEmpty(originalFieldPath);
        if (fieldPathIsNullOrEmpty || !ingestDocument.hasField(originalFieldPath, true)) {
            if (ignoreMissing) {
                // TODO refine this logic, now when there is missing field, it skip the field
                return null;
            } else if (fieldPathIsNullOrEmpty) {
                throw new IllegalArgumentException("field_map path cannot be null nor empty");
            } else {
                throw new IllegalArgumentException("field_map [" + originalFieldPath + "] doesn't exist");
            }
        }
        return originalFieldPath;
    }

    private void appendFieldValue(
        ModelTensorOutput modelTensorOutput,
        String originalModelOutputFieldName,
        String newModelOutputFieldName,
        IngestDocument ingestDocument
    ) {
        Object modelOutputValue = getModelOutputField(modelTensorOutput, originalModelOutputFieldName, ignoreMissing);

        if (modelOutputValue == null) {
            throw new RuntimeException("Cannot find model inference output for " + originalModelOutputFieldName);
        }
        ValueSource ingestValue = ValueSource.wrap(modelOutputValue, scriptService);
        TemplateScript.Factory ingestField = ConfigurationUtils
            .compileTemplate(TYPE, tag, newModelOutputFieldName, newModelOutputFieldName, scriptService);
        ingestDocument.appendFieldValue(ingestField, ingestValue, false);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        private final ScriptService scriptService;
        private final Client client;
        private final MLModelCacheHelper modelCacheHelper;

        public Factory(ScriptService scriptService, Client client, MLModelCacheHelper modelCacheHelper) {
            this.scriptService = scriptService;
            this.client = client;
            this.modelCacheHelper = modelCacheHelper;
        }

        @Override
        public MLInferenceProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config

        ) throws Exception {
            String model_id = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
            Map<String, String> model_config = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
            List<Map<String, String>> input_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
            List<Map<String, String>> output_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);
//            TODO fix modelCacheHelper is null
//            MLModel cachedMlModel = this.modelCacheHelper.getModelInfo(model_id);
//            FunctionName functionName = cachedMlModel.getAlgorithm();
//
//            // currently ml inference processor only support remote models
//            if (functionName != FunctionName.REMOTE) {
//                throw new IllegalArgumentException(
//                    "the provided model_id" + model_id + " is not a remote model. Please use a remote model_id."
//                );
//            }

            return new MLInferenceProcessor(
                model_id,
                input_map,
                output_map,
                model_config,
                processorTag,
                description,
                ignoreMissing,
                scriptService,
                client,
                modelCacheHelper
            );
        }
    }

}
