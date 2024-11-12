/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ml.query;

import static org.opensearch.ml.query.TemplateQueryBuilder.NAME;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.ml.searchext.MLInferenceRequestParameters;
import org.opensearch.ml.searchext.MLInferenceRequestParametersExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class TemplateQueryBuilderTests extends OpenSearchTestCase {

    protected TemplateQueryBuilder doCreateTestQueryBuilder() {
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);
        return new TemplateQueryBuilder(template);
    }

    @SneakyThrows
    public void testFromXContent() {
        /*
            {
              "template": {
                "term": {
                  "message": {
                    "value": "foo"
                  }
                }
              }
            }
        */
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(template);

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        TemplateQueryBuilder templateQueryBuilder = TemplateQueryBuilder.fromXContent(contentParser);

        assertEquals(NAME, templateQueryBuilder.NAME);
        assertEquals(template, templateQueryBuilder.getContent());

        SearchSourceBuilder source = new SearchSourceBuilder().query(templateQueryBuilder);
        assertEquals(source.toString(), "{\"query\":{\"template\":{\"term\":{\"message\":{\"value\":\"foo\"}}}}}");
    }

    public void testQuerySource() {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);
        QueryBuilder incomingQuery = new TemplateQueryBuilder(template);
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        assertEquals(source.toString(), "{\"query\":{\"template\":{\"term\":{\"message\":{\"value\":\"foo\"}}}}}");
    }

    public void testQuerySourceWithPlaceHolders() {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "${ext.inference.query_text}");
        term.put("message", message);
        template.put("term", term);
        QueryBuilder incomingQuery = new TemplateQueryBuilder(template);

        Map<String, Object> params = new HashMap<>();
        params.put("query_text", "foo");
        MLInferenceRequestParameters requestParameters = new MLInferenceRequestParameters(params);

        MLInferenceRequestParametersExtBuilder extBuilder = new MLInferenceRequestParametersExtBuilder();
        extBuilder.setRequestParameters(requestParameters);

        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery).ext(List.of(extBuilder));
        assertEquals(
            source.toString(),
            "{\"query\":{\"template\":{\"term\":{\"message\":{\"value\":\"${ext.inference.query_text}\"}}}},\"ext\":{\"ml_inference\":{\"query_text\":\"foo\"}}}"
        );
    }

    /**
     *
     * test geo query in template query
     * String jsonString = "{\n"
     *                 + "    \"template\": {\n"
     *                 + "      \"geo_shape\": {\n"
     *                 + "        \"location\": {\n"
     *                 + "          \"shape\": {\n"
     *                 + "            \"type\": \"Envelope\",\n"
     *                 + "            \"coordinates\": ${modelPredictionOutcome}\n"
     *                 + "          },\n"
     *                 + "          \"relation\": \"intersects\"\n"
     *                 + "        },\n"
     *                 + "        \"ignore_unmapped\": false,\n"
     *                 + "        \"boost\": 42.0\n"
     *                 + "      }\n"
     *                 + "    }\n"
     *                 + "  }";
     * @throws IOException
     */

    public void testFromJson() throws IOException {
        String jsonString = "{\n"
            + "    \"geo_shape\": {\n"
            + "      \"location\": {\n"
            + "        \"shape\": {\n"
            + "          \"type\": \"Envelope\",\n"
            + "          \"coordinates\": \"${modelPredictionOutcome}\"\n"
            + "        },\n"
            + "        \"relation\": \"intersects\"\n"
            + "      },\n"
            + "      \"ignore_unmapped\": false,\n"
            + "      \"boost\": 42.0\n"
            + "    }\n"
            + "}";

        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonString);
        parser.nextToken(); // Move to the start object token
        TemplateQueryBuilder parsed = TemplateQueryBuilder.fromXContent(parser);

        // Check if the parsed query is an instance of TemplateQueryBuilder
        assertNotNull(parsed);
        assertTrue(parsed instanceof TemplateQueryBuilder);

        // Check if the content of the parsed query matches the expected content
        Map<String, Object> expectedContent = new HashMap<>();
        Map<String, Object> geoShape = new HashMap<>();
        Map<String, Object> location = new HashMap<>();
        Map<String, Object> shape = new HashMap<>();

        shape.put("type", "Envelope");
        shape.put("coordinates", "${modelPredictionOutcome}");
        location.put("shape", shape);
        location.put("relation", "intersects");
        geoShape.put("location", location);
        geoShape.put("ignore_unmapped", false);
        geoShape.put("boost", 42.0);
        expectedContent.put("geo_shape", geoShape);

        // The actual content is wrapped in a "template" object
        Map<String, Object> actualContent = new HashMap<>();
        actualContent.put("template", expectedContent);
        // throwing error here --fixed
        assertEquals(expectedContent, parsed.getContent());

        // Test that the query can be serialized and deserialized
        BytesStreamOutput out = new BytesStreamOutput();
        parsed.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TemplateQueryBuilder deserializedQuery = new TemplateQueryBuilder(in);
        // throwing error here
        assertEquals(parsed.getContent(), deserializedQuery.getContent());

        // Test that the query can be converted to XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        parsed.doXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> expectedJson = new HashMap<>();
        Map<String, Object> template = new HashMap<>();
        template.put("geo_shape", geoShape);
        expectedJson.put("template", template);

        XContentParser jsonParser = XContentType.JSON
            .xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, builder.toString());
        Map<String, Object> actualJson = jsonParser.map();

        assertEquals(expectedJson, actualJson);
        System.out.println(StringUtils.toJson(actualJson));
    }

}
