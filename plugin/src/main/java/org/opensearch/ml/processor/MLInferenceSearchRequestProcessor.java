package org.opensearch.ml.processor;


import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.util.Map;

/**
 * MLInferenceSearchRequestProcessor requires a modelId string to call model inferences
 * maps fields in document for model input, and maps model inference output to the query fields
 * this processor also handles dot path notation for nested object( map of array) by rewriting json path accordingly
 */
public class MLInferenceSearchRequestProcessor extends AbstractProcessor implements SearchRequestProcessor, ModelExecutor {
    protected MLInferenceSearchRequestProcessor(String tag, String description, boolean ignoreFailure) {
        super(tag, description, ignoreFailure);
    }

    /**
     * Process a SearchRequest without receiving request-scoped state.
     * Implement this method if the processor makes no asynchronous calls.
     *
     * @param request the search request (which may have been modified by an earlier processor)
     * @return the modified search request
     * @throws Exception implementation-specific processing exception
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        String queryBody = request.source().toString();
        String queryString = StringUtils.toJson(queryBody);

        return null;
    }
    private <T> SearchRequest buildSearchRequest(String query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        XContentParser queryParser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, query);
        searchSourceBuilder.parseXContent(queryParser);
        searchSourceBuilder.fetchSource(sourceFields, null);
        searchSourceBuilder.size(docSize);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(index);
        return searchRequest;
    }
    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return null;
    }
}
