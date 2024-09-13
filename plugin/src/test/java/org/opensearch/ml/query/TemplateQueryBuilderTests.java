/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ml.query;

import static org.opensearch.ml.query.TemplateQueryBuilder.NAME;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;
//import org.opensearch.index.query.RandomQueryBuilder;

public class TemplateQueryBuilderTests extends OpenSearchTestCase {

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

}

// public class TemplateQueryBuilderTests extends AbstractQueryTestCase<TemplateQueryBuilder> {
//
//
// /**
// * Create the query that is being tested
// */
// @Override
// protected TemplateQueryBuilder doCreateTestQueryBuilder() {
// Object content = createInnerQuery(random());
// return new TemplateQueryBuilder(content);
// }
//
// private static Object createInnerQuery(Random r) {
// Object content = null;
// switch (RandomNumbers.randomIntBetween(r, 0, 3)) {
// case 0:
// content = MatchAllQueryBuilderTests().createTestQueryBuilder();
// case 1:
// content= TermQueryBuilderTests().createTestQueryBuilder();
// case 2:
// // We make sure this query has no types to avoid deprecation warnings in the
// // tests that use this method.
// content= IdsQueryBuilderTests().createTestQueryBuilder();
// case 3:
// content= createMultiTermQuery(r);
// default:
// throw new UnsupportedOperationException();
// }
// return content;
// }
//
// /**
// * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder}
// * and {@link QueryShardContext}. Contains the query specific checks to be implemented by subclasses.
// *
// * @param queryBuilder
// * @param query
// * @param context
// */
// @Override
// protected void doAssertLuceneQuery(TemplateQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
//
// }
// }
