package org.opensearch.ml.engine.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.engine.tools.AbstractRetrieverTool.*;
import static org.opensearch.ml.engine.tools.VectorDBTool.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchModule;

import lombok.SneakyThrows;

public class VectorDBToolTests extends AbstractRetrieverToolTests {
    @Mock
    private Client client;

    private VectorDBTool vectorDBTool;
    static public final String TEST_QUERY_TEST = "text";
    static public final String TEST_MODEL_ID = "test-model";
    static public final String TEST_EMBEDDING_FIELD = "embedding";
    static public final String TEST_QUERY = "{\"query\":{\"neural\":{\""
        + TEST_EMBEDDING_FIELD
        + "\":{\"query_text\":\""
        + TEST_QUERY_TEST
        + "\",\"model_id\":\""
        + TEST_MODEL_ID
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
        vectorDBTool = new VectorDBTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_MODEL_ID
        );
    }

    @Test
    public void testGetAttributes() {
        assertEquals(vectorDBTool.getType(), TYPE);
        assertEquals(vectorDBTool.getModelId(), TEST_MODEL_ID);
        assertEquals(vectorDBTool.getIndex(), TEST_INDEX);
        assertSame(vectorDBTool.getK(), DEFAULT_K);
        assertEquals(vectorDBTool.getEmbeddingField(), TEST_EMBEDDING_FIELD);
        assertEquals(vectorDBTool.getDocSize(), TEST_DOC_SIZE);
        assertEquals(vectorDBTool.getSourceFields(), TEST_SOURCE_FIELDS);
    }

    @Test
    public void testSetName() {
        assertEquals(vectorDBTool.getName(), TYPE);
        vectorDBTool.setName("new-name");
        assertEquals(vectorDBTool.getName(), "new-name");
    }

    @Test
    public void testGetQueryBodySuccess() {
        String testSuccessQuery = vectorDBTool.getQueryBody(TEST_QUERY_TEST);
        assertEquals(testSuccessQuery, TEST_QUERY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryBodyFailureOnEmbeddingField() {

        VectorDBTool vectorDBToolNoEmbeddingField = new VectorDBTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            "",
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_MODEL_ID
        );

        vectorDBToolNoEmbeddingField.getQueryBody(TEST_QUERY_TEST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryBodyFailureOnModelId() {

        VectorDBTool vectorDBToolNoModelId = new VectorDBTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            ""
        );
        vectorDBToolNoModelId.getQueryBody(TEST_QUERY_TEST);
    }

    @Test
    public void testFactory() {
        VectorDBTool.Factory factory = new VectorDBTool.Factory();
        assertNotNull(factory.getInstance());

        Map<String, Object> params = new HashMap<>();
        params.put(INDEX_FIELD, TEST_INDEX);
        params.put(EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(VectorDBTool.SOURCE_FIELD, gson.toJson(TEST_SOURCE_FIELDS));
        params.put(MODEL_ID_FIELD, TEST_MODEL_ID);
        params.put(DOC_SIZE_FIELD, String.valueOf(TEST_DOC_SIZE));
        VectorDBTool tool = factory.getInstance().create(params);

        assertEquals(TEST_INDEX, tool.getIndex());
        assertEquals(TEST_EMBEDDING_FIELD, tool.getEmbeddingField());
        assertEquals(TEST_SOURCE_FIELDS, tool.getSourceFields());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(TEST_DOC_SIZE, tool.getDocSize());
        assertEquals(VectorDBTool.TYPE, tool.getType());
        assertEquals(VectorDBTool.TYPE, tool.getName());
        assertEquals(VectorDBTool.DEFAULT_DESCRIPTION, VectorDBTool.Factory.getInstance().getDefaultDescription());
    }

    @Test
    @Override
    public void testRunAsyncWithSearchResults() {}

    @Test
    @Override
    public void testRunAsyncWithEmptySearchResponse() {}

    @Test
    @Override
    public void testRunAsyncWithIllegalQueryThenThrowException() {}

}
