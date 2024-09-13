package org.opensearch.ml.query;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

public class TemplateQueryBuilder extends AbstractQueryBuilder<TemplateQueryBuilder> {
    public static final String NAME = "template";
    private final Map<String, Object> content;

    public TemplateQueryBuilder(Map<String, Object> content) {
        this.content = content;
    }

    public static TemplateQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return new TemplateQueryBuilder(parser.map());
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public TemplateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.content = in.readMap();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(content);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, content);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new IllegalStateException("Template query should run with a ml_inference request processor");
    }

    @Override
    protected boolean doEquals(TemplateQueryBuilder other) {
        return Objects.equals(this.content, other.content);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(content);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        // TODO check empty place holder
        throw new IllegalStateException("Template query should run with a ml_inference request processor");

    }
}
