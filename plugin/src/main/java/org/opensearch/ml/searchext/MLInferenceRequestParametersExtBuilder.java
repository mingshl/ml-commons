/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ml.searchext;

import static org.opensearch.ml.searchext.MLInferenceRequestParameters.ML_INFERENCE_FIELD;

import java.io.IOException;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

public class MLInferenceRequestParametersExtBuilder extends SearchExtBuilder {
    private static final Logger logger = LogManager.getLogger(MLInferenceRequestParametersExtBuilder.class);
    public static final String NAME = ML_INFERENCE_FIELD;
    private MLInferenceRequestParameters requestParameters;

    public MLInferenceRequestParametersExtBuilder() {}

    public MLInferenceRequestParametersExtBuilder(StreamInput input) throws IOException {
        requestParameters = new MLInferenceRequestParameters(input);
    }

    public MLInferenceRequestParameters getRequestParameters() {
        return requestParameters;
    }

    public void setRequestParameters(MLInferenceRequestParameters requestParameters) {
        this.requestParameters = requestParameters;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.requestParameters);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MLInferenceRequestParametersExtBuilder)) {
            return false;
        }
        MLInferenceRequestParametersExtBuilder o = (MLInferenceRequestParametersExtBuilder) obj;
        return this.requestParameters.equals(o.requestParameters);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        requestParameters.writeTo(out);
    }

    // TODO
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // return builder.field(NAME,requestParameters.getParams());
        // builder.startObject(NAME);
        //
        // // for (Object config : requestParameters.getParams()) {
        // builder.field(PARAMS_FIELD, requestParameters.getParams());
        // // }
        // builder.endObject();
        // return builder;

        return builder.value(requestParameters);
    }

    public static MLInferenceRequestParametersExtBuilder parse(XContentParser parser) throws IOException {

        MLInferenceRequestParametersExtBuilder extBuilder = new MLInferenceRequestParametersExtBuilder();
        MLInferenceRequestParameters requestParameters = MLInferenceRequestParameters.parse(parser);
        extBuilder.setRequestParameters(requestParameters);
        return extBuilder;

        // MLInferenceRequestParametersExtBuilder extBuilder = new MLInferenceRequestParametersExtBuilder();
        // assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        // parser.nextToken();
        // if(parser.currentToken().name() == NAME){
        // MLInferenceRequestParameters requestParameters = MLInferenceRequestParameters.parse(parser);
        // extBuilder.setRequestParameters(requestParameters);}
        // else{
        // parser.nextToken();
        // }
        // return extBuilder;

        // MLInferenceRequestParametersExtBuilder builder = new MLInferenceRequestParametersExtBuilder();
        //// MLInferenceRequestParameters params = MLInferenceRequestParameters.parse(parser);
        //// builder.setRequestParameters(params);
        //
        //
        // XContentParser.Token token = parser.currentToken();
        // String currentFieldName = null;
        // if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
        // throw new ParsingException(
        // parser.getTokenLocation(),
        // "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]",
        // parser.getTokenLocation()
        // );
        // }
        // while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        // if (token == XContentParser.Token.FIELD_NAME) {
        // currentFieldName = parser.currentName();
        // } else if (token == XContentParser.Token.START_OBJECT) {
        // if (currentFieldName == NAME) {
        // currentFieldName = null;
        // while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        // if (token == XContentParser.Token.FIELD_NAME) {
        // currentFieldName = parser.currentName();
        // } else if (currentFieldName != null) {
        // if (currentFieldName == PARAMS_FIELD) {
        // token = parser.nextToken();
        // MLInferenceRequestParameters params = MLInferenceRequestParameters.parse(parser);
        // builder.setRequestParameters(params);
        // break;
        // } else {
        // throw new IllegalArgumentException(
        // "Unrecognized Result Transformer type [" + currentFieldName + "]");
        // }
        // }
        // }
        // } else {
        // throw new IllegalArgumentException("Unrecognized Transformer type [" + currentFieldName + "]");
        // }
        // } else {
        // throw new ParsingException(
        // parser.getTokenLocation(),
        // "Unknown key for a " + token + " in [" + currentFieldName + "].",
        // parser.getTokenLocation()
        // );
        // }
        // }
        //// return extBuilder;
        //
        //

        // return builder;

    }
}
