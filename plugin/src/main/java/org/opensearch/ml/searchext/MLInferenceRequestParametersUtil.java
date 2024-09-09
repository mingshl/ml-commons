/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ml.searchext;

import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.SearchExtBuilder;

public class MLInferenceRequestParametersUtil {

    public static MLInferenceRequestParameters getMLInferenceRequestParameters(SearchRequest searchRequest) {
        MLInferenceRequestParametersExtBuilder personalizeRequestParameterExtBuilder = null;
        if (searchRequest.source() != null && searchRequest.source().ext() != null && !searchRequest.source().ext().isEmpty()) {
            List<SearchExtBuilder> extBuilders = searchRequest
                .source()
                .ext()
                .stream()
                .filter(extBuilder -> MLInferenceRequestParametersExtBuilder.NAME.equals(extBuilder.getWriteableName()))
                .collect(Collectors.toList());

            if (!extBuilders.isEmpty()) {
                personalizeRequestParameterExtBuilder = (MLInferenceRequestParametersExtBuilder) extBuilders.get(0);
            }
        }
        MLInferenceRequestParameters mlInferenceRequestParameters = null;
        if (personalizeRequestParameterExtBuilder != null) {
            mlInferenceRequestParameters = personalizeRequestParameterExtBuilder.getRequestParameters();
        }
        return mlInferenceRequestParameters;
    }
}
