package org.opensearch.ml.engine.tools;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.engine.tools.AbstractRetrieverToolTests.TEST_XCONTENT_REGISTRY_FOR_QUERY;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Client;

import lombok.SneakyThrows;

public class NeuralSparseToolTests {
    public static final String TEST_EMBEDDING_FIELD = "test embedding";
    public static final String TEST_MODEL_ID = "123fsd23134";
    private Map<String, Object> params = new HashMap<>();

    @Before
    public void setup() {
        params.put(NeuralSparseTool.INDEX_FIELD, AbstractRetrieverToolTests.TEST_INDEX);
        params.put(NeuralSparseTool.EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(NeuralSparseTool.SOURCE_FIELD, gson.toJson(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS));
        params.put(NeuralSparseTool.MODEL_ID_FIELD, TEST_MODEL_ID);
        params.put(NeuralSparseTool.DOC_SIZE_FIELD, AbstractRetrieverToolTests.TEST_DOC_SIZE.toString());
    }

    @Test
    public void testSetName() {
        NeuralSparseTool tool = NeuralSparseTool.Factory.getInstance().create(params);
        assertEquals(tool.getName(), tool.TYPE);
        tool.setName("new-name");
        assertEquals(tool.getName(), "new-name");
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        NeuralSparseTool tool = NeuralSparseTool.Factory.getInstance().create(params);

        assertEquals(AbstractRetrieverToolTests.TEST_INDEX, tool.getIndex());
        assertEquals(TEST_EMBEDDING_FIELD, tool.getEmbeddingField());
        assertNotNull(NeuralSparseTool.Factory.getInstance());
        assertEquals(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS, tool.getSourceFields());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(AbstractRetrieverToolTests.TEST_DOC_SIZE, tool.getDocSize());
        assertEquals("NeuralSparseTool", tool.getType());
        assertEquals("NeuralSparseTool", tool.getName());
        assertEquals("Use this tool to search data in OpenSearch index.", NeuralSparseTool.Factory.getInstance().getDefaultDescription());

        Client client = mock(Client.class);
        NeuralSparseTool tool1 = NeuralSparseTool
            .builder()
            .client(client)
            .xContentRegistry(TEST_XCONTENT_REGISTRY_FOR_QUERY)
            .index(AbstractRetrieverToolTests.TEST_INDEX)
            .embeddingField(TEST_EMBEDDING_FIELD)
            .sourceFields(gson.toJson(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS).split(","))
            .modelId(TEST_MODEL_ID)
            .docSize(Integer.valueOf(AbstractRetrieverToolTests.TEST_DOC_SIZE.toString()))
            .build();

        assertEquals(tool1.getIndex(), tool.getIndex());
        assertEquals(tool1.getEmbeddingField(), tool.getEmbeddingField());
        assertEquals(tool1.getModelId(), tool.getModelId());
        assertEquals(tool1.getDocSize(), tool.getDocSize());
        assertEquals(tool1.getType(), tool.getType());
        assertEquals(tool1.getType(), tool.getName());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateToolFailure() {
        NeuralSparseTool tool = NeuralSparseTool.Factory.getInstance().create(null);
    }

    @Test
    @SneakyThrows
    public void testGetQueryBody() {
        NeuralSparseTool tool = NeuralSparseTool.Factory.getInstance().create(params);
        assertEquals(
            "{\"query\":{\"neural_sparse\":{\"test embedding\":{\""
                + "query_text\":\"{\"query\":{\"match_all\":{}}}\",\"model_id\":\"123fsd23134\"}}} }",
            tool.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithIllegalParams() {
        Map<String, Object> illegalParams1 = new HashMap<>(params);
        illegalParams1.remove(NeuralSparseTool.MODEL_ID_FIELD);
        NeuralSparseTool tool1 = NeuralSparseTool.Factory.getInstance().create(illegalParams1);
        assertThrows(
            "Parameter [embedding_field] and [model_id] can not be null or empty.",
            IllegalArgumentException.class,
            () -> tool1.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );

        Map<String, Object> illegalParams2 = new HashMap<>(params);
        illegalParams1.remove(NeuralSparseTool.EMBEDDING_FIELD);
        NeuralSparseTool tool2 = NeuralSparseTool.Factory.getInstance().create(illegalParams1);
        assertThrows(
            "Parameter [embedding_field] and [model_id] can not be null or empty.",
            IllegalArgumentException.class,
            () -> tool2.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
    }
}
