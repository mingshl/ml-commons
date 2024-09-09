/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ml.searchext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class MLInferenceRequestParametersExtBuilderTests extends OpenSearchTestCase {
    @Test
    public void testXContentRoundTrip() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("query_text", "foo");
        MLInferenceRequestParameters requestParameters = new MLInferenceRequestParameters(params);
        MLInferenceRequestParametersExtBuilder mlInferenceExtBuilder = new MLInferenceRequestParametersExtBuilder();
        mlInferenceExtBuilder.setRequestParameters(requestParameters);
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference serialized = XContentHelper.toXContent(mlInferenceExtBuilder, xContentType, true);

        XContentParser parser = createParser(xContentType.xContent(), serialized);

        MLInferenceRequestParametersExtBuilder deserialized = MLInferenceRequestParametersExtBuilder.parse(parser);

        assertEquals(mlInferenceExtBuilder, deserialized);
    }

    @Test
    public void testStreamRoundTrip() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("query_text", "foo");
        MLInferenceRequestParameters requestParameters = new MLInferenceRequestParameters();
        requestParameters.setParams(params);
        MLInferenceRequestParametersExtBuilder mlInferenceExtBuilder = new MLInferenceRequestParametersExtBuilder();
        mlInferenceExtBuilder.setRequestParameters(requestParameters);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        mlInferenceExtBuilder.writeTo(bytesStreamOutput);

        MLInferenceRequestParametersExtBuilder deserialized = new MLInferenceRequestParametersExtBuilder(
            bytesStreamOutput.bytes().streamInput()
        );
        assertEquals(mlInferenceExtBuilder, deserialized);
    }
}
