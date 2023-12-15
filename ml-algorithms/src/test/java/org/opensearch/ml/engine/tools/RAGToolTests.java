package org.opensearch.ml.engine.tools;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.engine.tools.AbstractRetrieverTool.DEFAULT_K;
import static org.opensearch.ml.engine.tools.AbstractRetrieverTool.DOC_SIZE_FIELD;
import static org.opensearch.ml.engine.tools.AbstractRetrieverTool.INDEX_FIELD;
import static org.opensearch.ml.engine.tools.VectorDBTool.EMBEDDING_FIELD;
import static org.opensearch.ml.engine.tools.VectorDBTool.INPUT_FIELD;
import static org.opensearch.ml.engine.tools.VectorDBTool.MODEL_ID_FIELD;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;
import org.opensearch.search.SearchModule;

import lombok.SneakyThrows;

public class RAGToolTests extends AbstractRetrieverToolTests {
    @Mock
    private Client client;
    private String mockedSearchResponseString;
    private String mockedEmptySearchResponseString;
    private SearchResponse mockedEmptySearchResponse;
    private SearchResponse mockedSearchResponse;
    private RAGTool ragTool;
    static public final String TEST_QUERY_TEST = "text";
    static public final String TEST_EMBEDDING_MODEL_ID = "test-embedding-model";
    static public final String TEST_MODEL_ID = "test-model";
    static public final String TEST_EMBEDDING_FIELD = "embedding";
    static public final String TEST_QUERY = "{\"query\":{\"neural\":{\""
        + TEST_EMBEDDING_FIELD
        + "\":{\"query_text\":\""
        + TEST_QUERY_TEST
        + "\",\"model_id\":\""
        + TEST_EMBEDDING_MODEL_ID
        + "\",\"k\":"
        + DEFAULT_K
        + "}}}"
        + " }";;
    static public final NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_QUERY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    @Override
    @Before
    @SneakyThrows
    public void setup() {
        client = mock(Client.class);

        try (InputStream searchResponseIns = RAGTool.class.getResourceAsStream("retrieval_tool_search_response.json")) {
            if (searchResponseIns != null) {
                mockedSearchResponseString = new String(searchResponseIns.readAllBytes());
            }
        }
        try (InputStream searchResponseIns = RAGTool.class.getResourceAsStream("retrieval_tool_empty_search_response.json")) {
            if (searchResponseIns != null) {
                mockedEmptySearchResponseString = new String(searchResponseIns.readAllBytes());
            }
        }

        ragTool = new RAGTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_EMBEDDING_MODEL_ID,
            TEST_MODEL_ID
        );

        mockedEmptySearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedEmptySearchResponseString)
            );
        mockedSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedSearchResponseString)
            );
    }

    @Test
    public void testGetAttributes() {
        assertEquals(ragTool.getType(), RAGTool.TYPE);
        assertEquals(ragTool.getModelId(), TEST_MODEL_ID);
        assertEquals(ragTool.getIndex(), TEST_INDEX);
        assertSame(ragTool.getK(), DEFAULT_K);
        assertEquals(ragTool.getEmbeddingField(), TEST_EMBEDDING_FIELD);
        assertEquals(ragTool.getDocSize(), TEST_DOC_SIZE);
        assertEquals(ragTool.getSourceFields(), TEST_SOURCE_FIELDS);
    }

    @Test
    public void testSetName() {
        assertEquals(ragTool.getName(), RAGTool.TYPE);
        ragTool.setName("new-name");
        assertEquals(ragTool.getName(), "new-name");
    }

    @Test
    public void testSetDefaultQuery() {
        assertEquals(ragTool.getDefaultQuery(), null);
        ragTool.setDefaultQuery(TEST_QUERY);
        assertEquals(ragTool.getDefaultQuery(), TEST_QUERY);
    }

    @Test
    public void testGetQueryBodySuccess() {
        String testSuccessQuery = ragTool.getQueryBody(TEST_QUERY_TEST);
        assertEquals(testSuccessQuery, TEST_QUERY);
    }

    @Test
    public void testParseQueryFromInputSuccess() {
        String testSuccessQuery = ragTool.parseQueryFromInput(Map.of(INPUT_FIELD, "text"));
        assertEquals(testSuccessQuery, TEST_QUERY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryBodyFailureOnEmbeddingField() {
        RAGTool ragToolNoEmbeddingField = new RAGTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            "",
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_EMBEDDING_MODEL_ID,
            TEST_MODEL_ID
        );

        ragToolNoEmbeddingField.getQueryBody(TEST_QUERY_TEST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryBodyFailureOnEmbeddingModelId() {
        RAGTool ragToolNoEmbeddingModelId = new RAGTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            "",
            TEST_MODEL_ID
        );
        ragToolNoEmbeddingModelId.getQueryBody(TEST_QUERY_TEST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryBodyFailureOnModelId() {
        RAGTool ragToolNoModelId = new RAGTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_EMBEDDING_MODEL_ID,
            ""
        );
        ragToolNoModelId.getQueryBody(TEST_QUERY_TEST);
    }

    @Test
    public void testFactory() {
        RAGTool.Factory factory = new RAGTool.Factory();
        assertNotNull(factory.getInstance());

        Map<String, Object> params = new HashMap<>();
        params.put(INDEX_FIELD, TEST_INDEX);
        params.put(EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(VectorDBTool.SOURCE_FIELD, gson.toJson(TEST_SOURCE_FIELDS));
        params.put(MODEL_ID_FIELD, TEST_MODEL_ID);
        params.put(DOC_SIZE_FIELD, String.valueOf(TEST_DOC_SIZE));
        factory.init(client, NamedXContentRegistry.EMPTY);
        RAGTool tool = factory.getInstance().create(params);

        assertEquals(TEST_INDEX, tool.getIndex());
        assertEquals(TEST_EMBEDDING_FIELD, tool.getEmbeddingField());
        assertEquals(TEST_SOURCE_FIELDS, tool.getSourceFields());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(TEST_DOC_SIZE, tool.getDocSize());
        assertEquals(RAGTool.TYPE, tool.getType());
        assertEquals(RAGTool.TYPE, tool.getName());
        assertEquals(RAGTool.DEFAULT_DESCRIPTION, RAGTool.Factory.getInstance().getDefaultDescription());
    }

    @Test
    public void testValidate() {
        assertTrue(ragTool.validate(Map.of(INPUT_FIELD, "Hello?")));
        assertFalse(ragTool.validate(null));
        assertFalse(ragTool.validate(Map.of()));
    }

    @Test
    public void testRunWithRuntimeException() throws IOException {
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);

        ActionListener listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("Failed to search index"));
            return null;
        }).when(client).search(any(), any());
        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to search index", argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testRunWithIOException() {
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);

        ActionListener listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IOException("Failed to search index"));
            return null;
        }).when(client).search(any(), any());

        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);

        verify(listener).onFailure(any(IOException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to search index", argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testRunWithMLModelException() {
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);
        ActionListener listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockedEmptySearchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("Failed to run model " + TEST_MODEL_ID));
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);

        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to run model " + TEST_MODEL_ID, argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testRunWithNullOutputParser() {
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);
        ActionListener listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockedEmptySearchResponse);
            return null;
        }).when(client).search(any(), any());

        ModelTensorOutput mlModelTensorOutput = getModelTensorOutput();
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        ragTool.setOutputParser(null);
        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);
        verify(client).execute(any(), any(), any());
        verify(client).search(any(), any());
        ArgumentCaptor<ModelTensorOutput> argumentCaptor = ArgumentCaptor.forClass(ModelTensorOutput.class);
        verify(listener).onResponse(argumentCaptor.capture());
    }

    @Test
    @SneakyThrows
    @Override
    public void testRunAsyncWithEmptySearchResponse() {
        // set a mock query because neural query depends on neural-search plugin
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);

        ModelTensorOutput mlModelTensorOutput = getModelTensorOutput();
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());

            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedEmptySearchResponse);
            return null;
        }).when(client).search(any(), any());

        final CompletableFuture<String> future = new CompletableFuture<>();
        ActionListener<String> listener = ActionListener.wrap(r -> { future.complete(r); }, e -> { future.completeExceptionally(e); });

        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);

        future.join();
        verify(client).execute(any(), any(), any());
        verify(client).search(any(), any());

        assertEquals(null, gson.fromJson(future.get(), String.class));

    }

    @Test
    @SneakyThrows
    @Override
    public void testRunAsyncWithSearchResults() {
        // set a mock query because neural query depends on neural-search plugin
        ragTool.setDefaultQuery(AbstractRetrieverToolTests.TEST_QUERY);

        ModelTensorOutput mlModelTensorOutput = getModelTensorOutput();
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedSearchResponse);
            return null;
        }).when(client).search(any(), any());

        final CompletableFuture<String> future = new CompletableFuture<>();
        ActionListener<String> listener = ActionListener.wrap(r -> { future.complete(r); }, e -> { future.completeExceptionally(e); });

        ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello?"), listener);

        future.join();
        verify(client).execute(any(), any(), any());
        verify(client).search(any(), any());

        assertEquals(null, gson.fromJson(future.get(), String.class));
    }

    @Test
    @SneakyThrows
    @Override
    public void testRunAsyncWithIllegalQueryThenThrowException() {
        assertThrows(
            "[input] is null or empty, can not process it.",
            IllegalArgumentException.class,
            () -> ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, ""), null)
        );
        ;
        assertThrows(
            "[input] is null or empty, can not process it.",
            IllegalArgumentException.class,
            () -> ragTool.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "  "), null)
        );
        ;
        assertThrows(
            "[input] is null or empty, can not process it.",
            IllegalArgumentException.class,
            () -> ragTool.run(Map.of("test", "hello?"), null)
        );
        ;
    }

    private static ModelTensorOutput getModelTensorOutput() {
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("thought", "thought 1", "action", "action1")).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();
        return mlModelTensorOutput;
    }
}
