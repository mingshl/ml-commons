/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.ml.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class InferenceProcessorAttributes implements ToXContentObject, NamedWriteable {

    protected List<Map<String, String>> inputMaps;

    protected List<Map<String, String>> outputMaps;

    protected String modelId;
    protected int maxPredictionTask;

    protected Map<String, String> modelConfigMaps;
    public static final String MODEL_ID = "model_id";
    public static final String INPUT_MAP = "input_map";
    public static final String OUTPUT_MAP = "output_map";
    public static final String MODEL_CONFIG = "model_config";
    public static final String MAX_PREDICTION_TASKS = "max_prediction_tasks";

    /**
     *  Utility class containing shared parameters for MLModelIngest/SearchProcessors
     *  */

    public InferenceProcessorAttributes(
        String modelId,
        List<Map<String, String>> inputMaps,
        List<Map<String, String>> outputMaps,
        Map<String, String> modelConfigMaps,
        int maxPredictionTask
    ) {
        this.modelId = modelId;
        this.modelConfigMaps = modelConfigMaps;
        this.inputMaps = inputMaps;
        this.outputMaps = outputMaps;
        this.maxPredictionTask = maxPredictionTask;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
