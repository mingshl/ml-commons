/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.ml.processor.MLInferenceIngestProcessor.DEFAULT_OUTPUT_FIELD_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MLInferenceIngestProcessorTests extends OpenSearchTestCase {

    @Mock
    private Client client;
    @Mock
    private ScriptService scriptService;
    @Mock
    private BiConsumer<IngestDocument, Exception> handler;
    private static final String PROCESSOR_TAG = "inference";
    private static final String DESCRIPTION = "inference_test";
    private IngestDocument ingestDocument;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

    }

    private MLInferenceIngestProcessor createMLInferenceProcessor(
        String model_id,
        Map<String, String> model_config,
        List<Map<String, String>> input_map,
        List<Map<String, String>> output_map,
        boolean ignoreMissing
    ) {
        return new MLInferenceIngestProcessor(
            model_id,
            input_map,
            output_map,
            model_config,
            PROCESSOR_TAG,
            DESCRIPTION,
            ignoreMissing,
            scriptService,
            client
        );
    }

    public void testExecute_successful() throws Exception {
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, null, false);
        IngestDocument document = processor.execute(ingestDocument);

        assert document.getSourceAndMetadata().containsKey("key1");
    }

    /**
     * test nested object document with array of Map<String,String>
     */
    public void testExecute_nestedObjectStringDocumentSuccess() {

        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "chunks.chunk";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, inputMap, null, true);
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3))).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        ArrayList<Object> childDocuments = new ArrayList<>();
        Map<String, Object> childDocument1  = new HashMap<>();
        childDocument1.put("chunk","this is first");
        Map<String, Object> childDocument2  = new HashMap<>();
        childDocument2.put("chunk","this is second");
        childDocuments.add(childDocument1);
        childDocuments.add(childDocument2);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("chunks", childDocuments);

        IngestDocument nestedObjectIngestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        processor.execute(nestedObjectIngestDocument, handler);

        sourceAndMetadata.put(DEFAULT_OUTPUT_FIELD_NAME, List.of(ImmutableMap.of("response", Arrays.asList(1, 2, 3))));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(nestedObjectIngestDocument, ingestDocument1);
    }

    /**
     * test nested object document with array of Map<String,Object>,
     * the value Object is a Map<String,String>
     */
    public void testExecute_nestedObjectMapDocumentSuccess() throws Exception {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "chunks.chunk.text";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, inputMap, null, true);
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3))).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        ArrayList<Object> childDocuments = new ArrayList<>();
        Map<String, Object> childDocument1Text  = new HashMap<>();
        childDocument1Text.put("text","this is first");
        Map<String, Object> childDocument1  = new HashMap<>();
        childDocument1.put("chunk",childDocument1Text);

        Map<String, Object> childDocument2  = new HashMap<>();
        Map<String, Object> childDocument2Text  = new HashMap<>();
        childDocument2Text.put("text","this is second");
        childDocument2.put("chunk",childDocument2Text);

        childDocuments.add(childDocument1);
        childDocuments.add(childDocument2);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("chunks", childDocuments);

        IngestDocument nestedObjectIngestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        processor.execute(nestedObjectIngestDocument, handler);

        //match input dataset
        ModelExecutor modelExecutor = new ModelExecutor() {};
        ArgumentCaptor<MLPredictionTaskRequest> argumentCaptor = ArgumentCaptor.forClass(MLPredictionTaskRequest.class);
        verify(client).execute(any(),argumentCaptor.capture(),any());

        Map<String, String> inputParameters = new HashMap<>();
        ArrayList<Object> embedding_text = new ArrayList<>();
        embedding_text.add("this is first");
        embedding_text.add("this is second");
        inputParameters.put("inputs",modelExecutor.toString(embedding_text));

        MLPredictionTaskRequest expectedRequest = (MLPredictionTaskRequest) modelExecutor.getRemoteModelInferenceRequest(inputParameters,"model1");
        MLPredictionTaskRequest actualRequest =   argumentCaptor.getValue();

        RemoteInferenceInputDataSet expectedRemoteInputDataset = (RemoteInferenceInputDataSet) expectedRequest.getMlInput().getInputDataset();
        RemoteInferenceInputDataSet actualRemoteInputDataset = (RemoteInferenceInputDataSet) actualRequest.getMlInput().getInputDataset();

        assertEquals(expectedRemoteInputDataset.getParameters().get("inputs"),actualRemoteInputDataset.getParameters().get("inputs"));

        //match document
        sourceAndMetadata.put(DEFAULT_OUTPUT_FIELD_NAME, List.of(ImmutableMap.of("response", Arrays.asList(1, 2, 3))));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(nestedObjectIngestDocument, ingestDocument1);
    }


    public void testExecute_jsonPathWithMissingLeaves() throws Exception {


        Map<String, Object> sourceObject = getNestedObjectWithAnotherNestedObjectDocument();
        sourceObject.remove("chunks.1.chunk.text.0.context", "this is third");


        Configuration suppressExceptionConfiguration = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL,Option.SUPPRESS_EXCEPTIONS ).build();
        Object jsonObject = JsonPath.parse(sourceObject).json();
        JsonPath.parse(jsonObject).delete("$.chunks[1].chunk.text[0].context");
        ArrayList<Object>  value =JsonPath.using(suppressExceptionConfiguration).parse(jsonObject).read("chunks.*.chunk.text.*.context");

        assertEquals(value.size(),4);
        assertEquals(value.get(0),"this is first");
        assertEquals(value.get(1),"this is second");
        assertNull(value.get(2));
        assertEquals(value.get(3),"this is fourth");
    }
    public void testSuccess_findDotPathForArrayForRead(){
        Map<String, Object> sourceObject = getNestedObjectWithAnotherNestedObjectDocument();
        ModelExecutor modelExecutor = new ModelExecutor() {};
        String actualDotPath = modelExecutor.findDotPathForNestedObject(sourceObject,"chunks.chunk.text.context");
        assertEquals(actualDotPath,"chunks[*].chunk.text[*].context");
//        Configuration suppressExceptionConfiguration = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS).build();
//        DocumentContext documentContext = JsonPath.parse(sourceObject);
//        documentContext.put("$.chunks.[1].chunk.text.[1].embedding","embedding", "1");
//        String modifiedJson = documentContext.jsonString();
//        System.out.println(modifiedJson);
    }

    public void testSuccess_findDotPathForArrayForWrite(){
        //test nested object
        Map<String, Object> nestedSourceObject = getNestedObjectWithAnotherNestedObjectDocument();
        ModelExecutor modelExecutor = new ModelExecutor() {};
        String actualDotPath = modelExecutor.findDotPathForNestedObject(nestedSourceObject,"chunks.chunk.text");
        assertEquals(actualDotPath,"chunks[*].chunk.text[*]");

        List<String> dotPaths = modelExecutor.writeNewDotPathForNestedObject(nestedSourceObject,"chunks.chunk.text.embedding");
        System.out.print(dotPaths);
        System.out.print("\n");

        // test key value pair json
        Map<String, Object> sourceObject = this.ingestDocument.getSourceAndMetadata();
        String actualDotPath1 = modelExecutor.findDotPathForNestedObject(sourceObject,"key1");
        assertEquals(actualDotPath1,"key1");

        List<String> dotPaths1 = modelExecutor.writeNewDotPathForNestedObject(sourceObject,"key1.embedding");
        System.out.print(dotPaths1);
        System.out.print("\n");

    }

    /**
     * test nested object document with array of Map<String,Object>,
     * the value Object is a also a nested object,
     */
    public void testExecute_nestedObjectAndNestedObjectDocumentOutputInOneFieldSuccess() throws Exception {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "chunks.chunk.text.context";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, inputMap, null, true);
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3,4))).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        Map<String, Object> sourceAndMetadata = getNestedObjectWithAnotherNestedObjectDocument();

        IngestDocument nestedObjectIngestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        processor.execute(nestedObjectIngestDocument, handler);


        //match input dataset
        ModelExecutor modelExecutor = new ModelExecutor() {};
        ArgumentCaptor<MLPredictionTaskRequest> argumentCaptor = ArgumentCaptor.forClass(MLPredictionTaskRequest.class);
        verify(client).execute(any(),argumentCaptor.capture(),any());

        Map<String, String> inputParameters = new HashMap<>();
        ArrayList<Object> embedding_text = new ArrayList<>();
        embedding_text.add("this is first");
        embedding_text.add("this is second");
        embedding_text.add("this is third");
        embedding_text.add("this is fourth");
        inputParameters.put("inputs",modelExecutor.toString(embedding_text));

        MLPredictionTaskRequest expectedRequest = (MLPredictionTaskRequest) modelExecutor.getRemoteModelInferenceRequest(inputParameters,"model1");
        MLPredictionTaskRequest actualRequest =   argumentCaptor.getValue();

        RemoteInferenceInputDataSet expectedRemoteInputDataset = (RemoteInferenceInputDataSet) expectedRequest.getMlInput().getInputDataset();
        RemoteInferenceInputDataSet actualRemoteInputDataset = (RemoteInferenceInputDataSet) actualRequest.getMlInput().getInputDataset();

        assertEquals(expectedRemoteInputDataset.getParameters().get("inputs"),actualRemoteInputDataset.getParameters().get("inputs"));

        //match document
        sourceAndMetadata.put(DEFAULT_OUTPUT_FIELD_NAME, List.of(ImmutableMap.of("response", Arrays.asList(1, 2, 3,4))));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(nestedObjectIngestDocument, ingestDocument1);

    }



    public void testExecute_nestedObjectAndNestedObjectDocumentOutputInArraySuccess() throws Exception {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "chunks.chunk.text.context";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);

        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        String modelOutputPath = "response";
        String documentFieldName = "chunks.chunk.text.embedding";
        output.put(modelOutputPath, documentFieldName);
        outputMap.add(output);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, inputMap, outputMap, true);
        ArrayList<List<Integer>> modelPredictionOutput = new ArrayList<>();
        modelPredictionOutput.add(Arrays.asList(1));
        modelPredictionOutput.add(Arrays.asList(2));
        modelPredictionOutput.add(Arrays.asList(3));
        modelPredictionOutput.add(Arrays.asList(4));
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response",modelPredictionOutput)).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        Map<String, Object> sourceAndMetadata = getNestedObjectWithAnotherNestedObjectDocument();

        IngestDocument nestedObjectIngestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        processor.execute(nestedObjectIngestDocument, handler);

        //match output dataset
       assertEquals( nestedObjectIngestDocument.getFieldValue("chunks.0.chunk.text.0.embedding", Object.class),Arrays.asList(1));
       assertEquals( nestedObjectIngestDocument.getFieldValue("chunks.0.chunk.text.1.embedding", Object.class),Arrays.asList(2));
       assertEquals( nestedObjectIngestDocument.getFieldValue("chunks.1.chunk.text.0.embedding", Object.class),Arrays.asList(3));
       assertEquals( nestedObjectIngestDocument.getFieldValue("chunks.1.chunk.text.1.embedding", Object.class),Arrays.asList(4));
    }

    public void testExecute_InferenceException() throws Exception {
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, null, false);
        when(client.execute(any(), any())).thenThrow(new RuntimeException("Executing Model failed with exception"));
        try {
            processor.execute(ingestDocument, handler);
        } catch (RuntimeException e) {
            assertEquals("Executing Model failed with exception", e.getMessage());
        }
    }

    public void testExecute_InferenceOnFailure() throws Exception {
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, null, false);
        RuntimeException inferenceFailure = new RuntimeException("Executing Model failed with exception");

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(inferenceFailure);
            return null;
        }).when(client).execute(any(), any(), any());
        processor.execute(ingestDocument, handler);

        verify(handler).accept(eq(null), eq(inferenceFailure));

    }

    public void testExecute_AppendFieldValueExceptionOnResponse() throws Exception {
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        String originalOutPutFieldName = "response1";
        output.put(originalOutPutFieldName, "text_embedding");
        outputMap.add(output);
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, outputMap, false);

        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3))).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        try {
            processor.execute(ingestDocument, handler);

        } catch (IllegalArgumentException e) {
            assertEquals("model inference output can not find field name: " + originalOutPutFieldName, e.getMessage());
        }

    }

    public void testExecute_whenInputFieldNotFound_ExceptionWithIgnoreMissingFalse() {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "text";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "text_embedding");
        outputMap.add(output);
        Map<String, String> model_config = new HashMap<>();
        model_config.put("position_embedding_type", "absolute");

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", model_config, inputMap, outputMap, false);

        try {
            processor.execute(ingestDocument, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("field name in input_map: [" + originalFieldPath + "] doesn't exist", e.getMessage());
        }

    }

    public void testExecute_whenInputFieldNotFound_SuccessWithIgnoreMissingTrue() {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "text";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "text_embedding");
        outputMap.add(output);
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, inputMap, outputMap, true);

        processor.execute(ingestDocument, handler);
    }

    public void testExecute_whenEmptyInputField_ExceptionWithIgnoreMissingFalse() {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "text_embedding");
        outputMap.add(output);
        Map<String, String> model_config = new HashMap<>();
        model_config.put("position_embedding_type", "absolute");

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", model_config, inputMap, outputMap, false);

        try {
            processor.execute(ingestDocument, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("field name in input_map [ " + originalFieldPath + "] cannot be null nor empty", e.getMessage());
        }
    }

    public void testExecute_whenEmptyInputField_ExceptionWithIgnoreMissingTrue() {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "text_embedding");
        outputMap.add(output);
        Map<String, String> model_config = new HashMap<>();
        model_config.put("position_embedding_type", "absolute");

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", model_config, inputMap, outputMap, true);

        processor.execute(ingestDocument, handler);

    }

    public void testExecute_IOExceptionWithIgnoreMissingFalse() throws JsonProcessingException {
        List<Map<String, String>> inputMap = new ArrayList<>();
        Map<String, String> input = new HashMap<>();
        String originalFieldPath = "text";
        input.put(originalFieldPath, "inputs");
        inputMap.add(input);
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "text_embedding");
        outputMap.add(output);
        Map<String, String> model_config = new HashMap<>();
        model_config.put("position_embedding_type", "absolute");

        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.readValue(Mockito.anyString(), eq(Object.class))).thenThrow(JsonProcessingException.class);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", model_config, inputMap, outputMap, false);

        try {
            processor.execute(ingestDocument, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("field name in input_map: [" + originalFieldPath + "] doesn't exist", e.getMessage());
        }
    }

    public void testExecute_NoModelInput_Exception() {
        MLInferenceIngestProcessor processorIgnoreMissingTrue = createMLInferenceProcessor("model1", null, null, null, true);
        MLInferenceIngestProcessor processorIgnoreMissingFalse = createMLInferenceProcessor("model1", null, null, null, false);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        IngestDocument emptyIngestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        try {
            processorIgnoreMissingTrue.execute(emptyIngestDocument, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("wrong input. The model input cannot be empty.", e.getMessage());
        }
        try {
            processorIgnoreMissingFalse.execute(emptyIngestDocument, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("wrong input. The model input cannot be empty.", e.getMessage());
        }

    }

    public void testExecute_AppendModelOutputSuccess() {
        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, null, true);
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("response", Arrays.asList(1, 2, 3))).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        processor.execute(ingestDocument, handler);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        sourceAndMetadata.put(DEFAULT_OUTPUT_FIELD_NAME, List.of(ImmutableMap.of("response", Arrays.asList(1, 2, 3))));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(ingestDocument, ingestDocument1);
    }


    public void testExecute_getModelOutputFieldWithFieldNameSuccess() {
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response", "classification");
        outputMap.add(output);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, outputMap, true);
        ModelTensor modelTensor = ModelTensor
            .builder()
            .dataAsMap(ImmutableMap.of("response", ImmutableMap.of("language", "en", "score", "0.9876")))
            .build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        processor.execute(ingestDocument, handler);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        sourceAndMetadata.put("classification", List.of(ImmutableMap.of("language", "en", "score", "0.9876")));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(ingestDocument, ingestDocument1);
    }

    public void testExecute_getModelOutputFieldWithDotPathSuccess() {
        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response.language", "language_identification");
        outputMap.add(output);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, outputMap, true);
        ModelTensor modelTensor = ModelTensor
            .builder()
            .dataAsMap(ImmutableMap.of("response", ImmutableMap.of("language", "en", "score", "0.9876")))
            .build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());

        processor.execute(ingestDocument, handler);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        sourceAndMetadata.put("language_identification", List.of("en"));
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(ingestDocument, ingestDocument1);
    }

    public void testExecute_getModelOutputFieldWithInvalidDotPathSuccess() {

        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response.lan", "language_identification");
        outputMap.add(output);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, outputMap, true);
        ModelTensor modelTensor = ModelTensor
            .builder()
            .dataAsMap(ImmutableMap.of("response", ImmutableMap.of("language", "en", "score", "0.9876")))
            .build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());
        Map<String, Object> sourceAndMetadata1 = new HashMap<>();
        sourceAndMetadata1.put("key1", "value1");
        sourceAndMetadata1.put("key2", "value2");
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata1, new HashMap<>());
        processor.execute(ingestDocument1, handler);

        Map<String, Object> sourceAndMetadata2 = new HashMap<>();
        sourceAndMetadata2.put("key1", "value1");
        sourceAndMetadata2.put("key2", "value2");
        sourceAndMetadata2
            .put("language_identification", List.of(ImmutableMap.of("response", ImmutableMap.of("language", "en", "score", "0.9876"))));
        IngestDocument ingestDocument2 = new IngestDocument(sourceAndMetadata2, new HashMap<>());
        verify(handler).accept(eq(ingestDocument1), isNull());
        assertEquals(ingestDocument2, ingestDocument1);
    }

    public void testExecute_getModelOutputFieldWithInvalidDotPathException() {

        List<Map<String, String>> outputMap = new ArrayList<>();
        Map<String, String> output = new HashMap<>();
        output.put("response.lan", "language_identification");
        outputMap.add(output);

        MLInferenceIngestProcessor processor = createMLInferenceProcessor("model1", null, null, outputMap, false);
        ModelTensor modelTensor = ModelTensor
            .builder()
            .dataAsMap(ImmutableMap.of("response", ImmutableMap.of("language", "en", "score", "0.9876")))
            .build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(any(), any(), any());
        Map<String, Object> sourceAndMetadata1 = new HashMap<>();
        sourceAndMetadata1.put("key1", "value1");
        sourceAndMetadata1.put("key2", "value2");
        IngestDocument ingestDocument1 = new IngestDocument(sourceAndMetadata1, new HashMap<>());
        try {
            processor.execute(ingestDocument1, handler);
        } catch (IllegalArgumentException e) {
            assertEquals("model inference output can not find field name: " + "response.lan", e.getMessage());

        }
        ;

    }
    private static Map<String, Object> getNestedObjectWithAnotherNestedObjectDocument() {
        ArrayList<Object> childDocuments = new ArrayList<>();

        Map<String, Object> childDocument1Text  = new HashMap<>();
        ArrayList<Object> grandChildDocuments1 = new ArrayList<>();
        Map<String, Object> grandChildDocument1Text  = new HashMap<>();
        grandChildDocument1Text.put("context","this is first");
        grandChildDocument1Text.put("chapter","first chapter");
        Map<String, Object> grandChildDocument2Text  = new HashMap<>();
        grandChildDocument2Text.put("context","this is second");
        grandChildDocument2Text.put("chapter","first chapter");
        grandChildDocuments1.add(grandChildDocument1Text);
        grandChildDocuments1.add(grandChildDocument2Text);
        childDocument1Text.put("text",grandChildDocuments1);

        Map<String, Object> childDocument1  = new HashMap<>();
        childDocument1.put("chunk",childDocument1Text);

        Map<String, Object> childDocument2  = new HashMap<>();
        Map<String, Object> childDocument2Text  = new HashMap<>();
        ArrayList<Object> grandChildDocuments2 = new ArrayList<>();

        Map<String, Object> grandChildDocument3Text  = new HashMap<>();
        grandChildDocument3Text.put("context","this is third");
        grandChildDocument3Text.put("chapter","second chapter");
        Map<String, Object> grandChildDocument4Text  = new HashMap<>();
        grandChildDocument4Text.put("context","this is fourth");
        grandChildDocument4Text.put("chapter","first chapter");
        grandChildDocuments2.add(grandChildDocument3Text);
        grandChildDocuments2.add(grandChildDocument4Text);

        childDocument2Text.put("text",grandChildDocuments2);
        childDocument2.put("chunk",childDocument2Text);

        childDocuments.add(childDocument1);
        childDocuments.add(childDocument2);

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("chunks", childDocuments);
        return sourceAndMetadata;
    }
}
