/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//package org.opensearch.ml.processor;
//
//import static org.opensearch.ml.common.utils.StringUtils.gson;
//import static org.opensearch.ml.processor.MLModelUtil.*;
//
//import java.io.IOException;
//import java.util.*;
//
//import org.opensearch.action.ActionRequest;
//import org.opensearch.action.search.SearchRequest;
//import org.opensearch.action.search.SearchResponse;
//import org.opensearch.client.Client;
//import org.opensearch.common.collect.Tuple;
//import org.opensearch.common.document.DocumentField;
//import org.opensearch.common.xcontent.XContentHelper;
//import org.opensearch.core.action.ActionListener;
//import org.opensearch.core.common.bytes.BytesReference;
//import org.opensearch.core.xcontent.MediaType;
//import org.opensearch.core.xcontent.XContentBuilder;
//import org.opensearch.ingest.ConfigurationUtils;
//import org.opensearch.ml.common.output.model.ModelTensorOutput;
//import org.opensearch.ml.common.transport.MLTaskResponse;
//import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
//import org.opensearch.search.SearchHit;
//import org.opensearch.search.pipeline.AbstractProcessor;
//import org.opensearch.search.pipeline.Processor;
//import org.opensearch.search.pipeline.SearchResponseProcessor;
//
//import lombok.SneakyThrows;
//
//public class MLInferenceSearchResponseProcessorDraft extends AbstractProcessor implements SearchResponseProcessor, ModelExecutor {
//
////    private final MLModelUtil mlModelUtil;
//    private final boolean ignoreMissing;
//    private final boolean ignoreFailure;
//    private static Client client;
//    public static final String TYPE = "ml_inference";
//    public static final String DEFAULT_OUTPUT_FIELD_NAME = "inference_results";
//
//    protected MLInferenceSearchResponseProcessorDraft(
//        String model_id,
//        List<Map<String, String>> input_map,
//        List<Map<String, String>> output_map,
//        Map<String, String> model_config,
//        String tag,
//        String description,
//        boolean ignoreMissing,
//        boolean ignoreFailure,
//        Client client
//    ) {
//        super(tag, description, ignoreFailure);
//        this.mlModelUtil = new MLModelUtil(model_id, input_map, output_map, model_config);
//        this.ignoreMissing = ignoreMissing;
//        this.ignoreFailure = ignoreFailure;
//        this.client = client;
//    }
//
//    public boolean isIgnoreMissing() {
//        return ignoreMissing;
//    }
//
//    @Override
//    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
//        boolean foundField = false;
//
//        List<Map<String, String>> process_input = mlModelUtil.getInput_map();
//        List<Map<String, String>> process_output = mlModelUtil.getOutput_map();
//        int i = 0;
//        int round = (process_input != null) ? process_input.size() : 0;
//
//        return process_predictions(response, process_input, process_output, i, round);
//
//        // SearchHit[] hits = response.getHits().getHits();
//        //
//        // // step 1 get field for model input
//        // for (SearchHit hit : hits) {
//        // Map<String, DocumentField> fields = hit.getFields();
//        // if (fields.containsKey(oldField)) {
//        // foundField = true;
//        // DocumentField old = hit.removeDocumentField(oldField);
//        // DocumentField newDocField = new DocumentField(newField, old.getValues());
//        // hit.setDocumentField(newField, newDocField);
//        // }
//        //
//        // // step 2 send model input for model prediction
//        //
//        //
//        // // step 3 append prediction outcomes to hits
//        // if (!foundField && !ignoreMissing) {
//        // throw new IllegalArgumentException("Document with id " + hit.getId() + " is missing field " + oldField);
//        // }
//
//        // return null;
//    }
//
//    private SearchResponse process_predictions(
//        SearchResponse response,
//        List<Map<String, String>> process_input,
//        List<Map<String, String>> process_output,
//        int i,
//        int round
//    ) {
//
//        if (i >= round && i != 0) {
//            return response;
//        } else {
//            SearchHit[] hits = response.getHits().getHits();
//            Map<String, String> modelParameters = new HashMap<>();
//            if (mlModelUtil.getModel_config() != null) {
//                modelParameters.putAll(mlModelUtil.getModel_config());
//            }
//            // when no input mapping is provided, default to read all fields from documents as model input
//            if (round == 0) {
//                // TODO to handle multiple list of input map.
//
//                // List modelInputFieldList = new ArrayList();
//                // SearchHit[] hits = response.getHits().getHits();
//                // for (SearchHit hit : hits) {
//                // Map<String, DocumentField> fields = hit.getFields();
//                // if (fields.containsKey()) {
//                //
//                // }
//                // }
//                // Set<String> documentFields = ingestDocument.getSourceAndMetadata().keySet();
//                // for (String field : documentFields) {
//                // getMappedModelInputFromDocuments(ingestDocument, modelParameters, field, field);
//                // }
//
//            } else {
//                Map<String, String> inputMapping = process_input.get(i);
//                for (Map.Entry<String, String> entry : inputMapping.entrySet()) {
//                    String originalFieldName = entry.getKey();// dairy
//                    String ModelInputFieldName = entry.getValue();
//                    List modelInputFieldList = new ArrayList();
//
//                    for (SearchHit hit : hits) {
//                        Map<String, DocumentField> fields = hit.getFields();// fields size of 0
//                        if (fields.containsKey(originalFieldName)) {
//                            modelInputFieldList.add(hit.getFields().get(originalFieldName));
//                        }
//                    }
//                    modelParameters.put(ModelInputFieldName, getModelInputFieldValue(modelInputFieldList));
//                }
//
//            }
//            ActionRequest request = getRemoteModelInferenceResult(modelParameters, mlModelUtil.getModel_id());
//
//            client.execute(MLPredictionTaskAction.INSTANCE, request, new ActionListener<>() {
//
//                @SneakyThrows
//                @Override
//                public void onResponse(MLTaskResponse mlTaskResponse) {
//                    ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
//
//                    if (process_output == null || process_output.isEmpty()) {
//                        // ToDo handle not output list
//                        //
//                        // ArrayList<Object> modelTensorOutputList = new ArrayList<>();
//                        //
//                        // modelTensorOutputList.add(modelTensorOutput);
//                        // DocumentField newDocField = new DocumentField(DEFAULT_OUTPUT_FIELD_NAME, modelTensorOutputList);
//                        // for (SearchHit hit : hits) {
//                        // hit.setDocumentField(DEFAULT_OUTPUT_FIELD_NAME, newDocField);
//                        // }
//                    } else {
//                        Map<String, String> outputMapping = process_output.get(i);
//                        for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
//                            String originalModelOutputFieldName = entry.getKey(); // response
//                            String newModelOutputFieldName = entry.getValue(); // inference_result
//                            // appendFieldValue(modelTensorOutput, originalModelOutputFieldName, newModelOutputFieldName, ingestDocument);
//                            Object modelTensorOutputField = null;
//                            try {
//                                modelTensorOutputField = getModelOutputField(
//                                    modelTensorOutput,
//                                    originalModelOutputFieldName,
//                                    isIgnoreMissing()
//                                );
//                            } catch (IOException e) {
//                                throw new RuntimeException(e);
//                            }
//                            if (modelTensorOutputField instanceof ArrayList) {
//                                ArrayList<?> modelTensorOutputList = (ArrayList<?>) modelTensorOutputField;
//                                if (modelTensorOutputList.size() == hits.length) {
//                                    int i = 0;
//
//                                    for (SearchHit hit : hits) {
//                                        List<Object> modelTensorOutputDocument = new ArrayList<>();
//                                        modelTensorOutputDocument.add(modelTensorOutputList.get(i));
//                                        DocumentField newDocField = new DocumentField(DEFAULT_OUTPUT_FIELD_NAME, modelTensorOutputDocument);
//                                        hit.setDocumentField(DEFAULT_OUTPUT_FIELD_NAME, newDocField);
//
//                                        if (hit.hasSource()) {
//                                            BytesReference sourceRef = hit.getSourceRef();
//                                            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper
//                                                .convertToMap(sourceRef, false, (MediaType) null);
//
//                                            Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
//                                            if (!sourceAsMap.containsKey(originalModelOutputFieldName)) {
//                                                sourceAsMap.put(DEFAULT_OUTPUT_FIELD_NAME, modelTensorOutputDocument);
//
//                                                XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
//
//                                                builder.map(sourceAsMap);
//
//                                                hit.sourceRef(BytesReference.bytes(builder));
//                                            }
//                                        }
//
//                                        i++;
//                                    }
//                                }
//                            }
//                        }
//                    }
//                    try {
//                        process_predictions(response, process_input, process_output, i + 1, round);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        }
//
//        return response;
//    }
//
//    @Override
//    public String getType() {
//        return TYPE;
//    }
//
//    public static final class Factory implements Processor.Factory {
//
//        private final Client client;
//
//        public Factory(Client client) {
//            this.client = client;
//        }
//
//        @Override
//        public MLInferenceSearchResponseProcessor create(
//            Map registry,
//            String processorTag,
//            String description,
//            boolean ignoreFailure,
//            Map config,
//            PipelineContext pipelineContext
//        ) throws Exception {
//            String model_id = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, MODEL_ID);
//            Map<String, Object> model_config_input = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, MODEL_CONFIG);
//            List<Map<String, String>> input_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, INPUT_MAP);
//            List<Map<String, String>> output_map = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, OUTPUT_MAP);
//            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, false);
//
//            // convert model config user input data structure to Map<String, String>
//            Map<String, String> model_config = null;
//            if (model_config_input != null) {
//                model_config = new HashMap<>();
//                for (String key : model_config_input.keySet()) {
//                    model_config.put(key, gson.toJson(model_config_input.get(key)));
//                }
//            }
//
//            return new MLInferenceSearchResponseProcessor(
//                model_id,
//                input_map,
//                output_map,
//                model_config,
//                processorTag,
//                description,
//                ignoreMissing,
//                ignoreFailure,
//                client
//            );
//        }
//
//    }
//}
