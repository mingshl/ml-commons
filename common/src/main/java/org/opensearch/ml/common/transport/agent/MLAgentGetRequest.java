/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.transport.agent;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.ml.common.CommonValue.VERSION_2_19_0;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import lombok.Builder;
import lombok.Getter;

@Getter
public class MLAgentGetRequest extends ActionRequest {

    String agentId;
    // This is to identify if the get request is initiated by user or not. Sometimes during
    // delete/update options, we also perform get operation. This field is to distinguish between
    // these two situations.
    boolean isUserInitiatedGetRequest;
    String tenantId;

    @Builder
    public MLAgentGetRequest(String agentId, boolean isUserInitiatedGetRequest, String tenantId) {
        this.agentId = agentId;
        this.isUserInitiatedGetRequest = isUserInitiatedGetRequest;
        this.tenantId = tenantId;
    }

    public MLAgentGetRequest(StreamInput in) throws IOException {
        super(in);
        Version streamInputVersion = in.getVersion();
        this.agentId = in.readString();
        this.isUserInitiatedGetRequest = in.readBoolean();
        this.tenantId = streamInputVersion.onOrAfter(VERSION_2_19_0) ? in.readOptionalString() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Version streamOutputVersion = out.getVersion();
        out.writeString(this.agentId);
        out.writeBoolean(isUserInitiatedGetRequest);
        if (streamOutputVersion.onOrAfter(VERSION_2_19_0)) {
            out.writeOptionalString(tenantId);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException exception = null;

        if (this.agentId == null) {
            exception = addValidationError("ML agent id can't be null", exception);
        }

        return exception;
    }

    public static MLAgentGetRequest fromActionRequest(ActionRequest actionRequest) {
        if (actionRequest instanceof MLAgentGetRequest) {
            return (MLAgentGetRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new MLAgentGetRequest(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionRequest into MLAgentGetRequest", e);
        }
    }
}
