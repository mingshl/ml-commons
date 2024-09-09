/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ml.searchext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class MLInferenceRequestParameters implements Writeable, ToXContentObject {
    static final String ML_INFERENCE_FIELD = "ml_inference";
    public static final String PARAMS_FIELD = "params";

    private static final ObjectParser<MLInferenceRequestParameters, Void> PARSER;
    private static final ParseField PARAMS = new ParseField(PARAMS_FIELD);

    static {
        PARSER = new ObjectParser<>(ML_INFERENCE_FIELD, MLInferenceRequestParameters::new);
        PARSER.declareObject(MLInferenceRequestParameters::setParams, (XContentParser p, Void c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new IllegalArgumentException("Error parsing ml inference params from request parameters", e);
            }
        }, PARAMS);
    }
    private Map<String, Object> params;

    public MLInferenceRequestParameters() {}

    public MLInferenceRequestParameters(Map<String, Object> params) {
        this.params = params;
    }

    public MLInferenceRequestParameters(StreamInput input) throws IOException {
        this.params = input.readMap();
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    // public String toString() {
    // Map<String, Object> paramsMap = new HashMap<>();
    // paramsMap.put(PARAMS_FIELD,this.params);
    // Map<String, Object> requestParameters = new HashMap<>();
    // requestParameters.put(ML_INFERENCE_FIELD,paramsMap);
    // Map<String, Object> extMap = new HashMap<>();
    // extMap.put("ext",requestParameters);
    // return toJson(extMap);
    // }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(PARAMS.getPreferredName(), this.params);
    }

    public static MLInferenceRequestParameters parse(XContentParser parser) throws IOException {
        MLInferenceRequestParameters requestParameters = PARSER.parse(parser, null);
        return requestParameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MLInferenceRequestParameters config = (MLInferenceRequestParameters) o;

        return params.equals(config.getParams());
    }

    @Override
    public int hashCode() {
        return Objects.hash(params);
    }
}
