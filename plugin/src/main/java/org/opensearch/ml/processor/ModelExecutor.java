package org.opensearch.ml.processor;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import com.google.gson.Gson;

/**
 * General ModelExecutor interface.
 */
public interface ModelExecutor {

    default <T> ActionRequest getRemoteModelInferenceResult(Map<String, String> parameters, String modelId) {

        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(parameters).build();
        if (inputDataSet.getParameters() == null) {
            throw new IllegalArgumentException("wrong input");
        }
        MLInput mlInput = MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build();

        ActionRequest request = new MLPredictionTaskRequest(modelId, mlInput, null);

        return request;

    }

    default String getModelInputFieldValue(Object originalFieldValue) {
        Gson gson = new Gson();
        String originalFieldValueAsString = null;
        try {
            originalFieldValueAsString = AccessController
                .doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(originalFieldValue));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
        return originalFieldValueAsString;
    }

    /**
     * filter model outputs by field name,
     * default to get all prediction outputs
     */
    default Object getModelOutputField(ModelTensorOutput modelOutput, String fieldName, boolean ignoreMissing) {
        Map<String, ?> modelTensorOutputMap = modelOutput.getMlModelOutputs().get(0).getMlModelTensors().get(0).getDataAsMap();

        if (fieldName == null) {
            return modelTensorOutputMap;
        } else if (modelTensorOutputMap.containsKey(fieldName)) {
            return modelTensorOutputMap.get(fieldName);
        } else {
            if (ignoreMissing) {
                return modelTensorOutputMap;
            } else {
                throw new IllegalArgumentException("model inference output can not find field name: " + fieldName);
            }
        }
    }

}
