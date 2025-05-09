package org.opensearch.ml.common.transport.deploy;

import static org.junit.Assert.*;
import static org.opensearch.ml.common.CommonValue.VERSION_2_18_0;
import static org.opensearch.ml.common.CommonValue.VERSION_2_19_0;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;

public class MLDeployModelRequestTest {

    private MLDeployModelRequest mlDeployModelRequest;

    @Before
    public void setUp() throws Exception {
        mlDeployModelRequest = MLDeployModelRequest
            .builder()
            .modelId("modelId")
            .modelNodeIds(new String[] { "modelNodeIds" })
            .async(true)
            .dispatchTask(true)
            .build();

    }

    @Test
    public void testValidateWithBuilder() {
        MLDeployModelRequest request = MLDeployModelRequest.builder().modelId("modelId").build();
        assertNull(request.validate());
    }

    @Test
    public void testValidateWithoutBuilder() {
        MLDeployModelRequest request = new MLDeployModelRequest("modelId", null, true);
        assertNull(request.validate());
    }

    @Test
    public void validate_Exception_WithNullModelId() {
        MLDeployModelRequest request = MLDeployModelRequest
            .builder()
            .modelId(null)
            .modelNodeIds(new String[] { "modelNodeIds" })
            .async(true)
            .dispatchTask(true)
            .build();
        ActionRequestValidationException exception = request.validate();
        assertEquals("Validation Failed: 1: ML model id can't be null;", exception.getMessage());
    }

    @Test
    public void writeTo_Success() throws IOException {

        MLDeployModelRequest request = mlDeployModelRequest;
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        request.writeTo(bytesStreamOutput);
        request = new MLDeployModelRequest(bytesStreamOutput.bytes().streamInput());

        assertEquals("modelId", request.getModelId());
        assertArrayEquals(new String[] { "modelNodeIds" }, request.getModelNodeIds());
        assertTrue(request.isAsync());
        assertTrue(request.isDispatchTask());
    }

    @Test(expected = UncheckedIOException.class)
    public void fromActionRequest_IOException() {
        ActionRequest actionRequest = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new IOException("test");
            }
        };
        MLDeployModelRequest.fromActionRequest(actionRequest);
    }

    @Test
    public void fromActionRequest_Success_WithMLDeployModelRequest() {
        MLDeployModelRequest request = MLDeployModelRequest.builder().modelId("modelId").build();
        assertSame(MLDeployModelRequest.fromActionRequest(request), request);
    }

    @Test
    public void fromActionRequest_Success_WithNonMLDeployModelRequest() {
        MLDeployModelRequest request = mlDeployModelRequest;
        ActionRequest actionRequest = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                request.writeTo(out);
            }
        };
        MLDeployModelRequest result = MLDeployModelRequest.fromActionRequest(actionRequest);
        assertNotSame(result, request);
        assertEquals(request.isAsync(), result.isAsync());
        assertEquals(request.isDispatchTask(), result.isDispatchTask());
    }

    @Test
    public void testParse() throws Exception {
        String modelId = "modelId";
        String expectedInputStr = "{\"node_ids\":[\"modelNodeIds\"]}";
        parseFromJsonString(modelId, expectedInputStr, parsedInput -> {
            assertEquals("modelId", parsedInput.getModelId());
            assertArrayEquals(new String[] { "modelNodeIds" }, parsedInput.getModelNodeIds());
            assertFalse(parsedInput.isAsync());
            assertTrue(parsedInput.isDispatchTask());
        });
    }

    @Test
    public void testParseWithInvalidField() throws Exception {
        String modelId = "modelId";
        String withInvalidFieldInputStr =
            "{\"void\":\"void\", \"dispatchTask\":\"false\", \"async\":\"true\", \"node_ids\":[\"modelNodeIds\"]}";
        parseFromJsonString(modelId, withInvalidFieldInputStr, parsedInput -> {
            assertEquals("modelId", parsedInput.getModelId());
            assertArrayEquals(new String[] { "modelNodeIds" }, parsedInput.getModelNodeIds());
            assertFalse(parsedInput.isAsync());
            assertTrue(parsedInput.isDispatchTask());
        });
    }

    @Test
    public void testStreamInputVersionBefore_2_19_0() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(VERSION_2_18_0);
        mlDeployModelRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(VERSION_2_18_0);
        MLDeployModelRequest request = new MLDeployModelRequest(in);

        assertEquals(mlDeployModelRequest.getModelId(), request.getModelId());
        assertArrayEquals(mlDeployModelRequest.getModelNodeIds(), request.getModelNodeIds());
        assertEquals(mlDeployModelRequest.isAsync(), request.isAsync());
        assertEquals(mlDeployModelRequest.isDispatchTask(), request.isDispatchTask());
        assertNull(request.getTenantId());
    }

    @Test
    public void testStreamInputVersionAfter_2_19_0() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(VERSION_2_19_0);
        mlDeployModelRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(VERSION_2_19_0);
        MLDeployModelRequest request = new MLDeployModelRequest(in);

        assertEquals(mlDeployModelRequest.getModelId(), request.getModelId());
        assertArrayEquals(mlDeployModelRequest.getModelNodeIds(), request.getModelNodeIds());
        assertEquals(mlDeployModelRequest.isAsync(), request.isAsync());
        assertEquals(mlDeployModelRequest.isDispatchTask(), request.isDispatchTask());
        assertEquals(mlDeployModelRequest.getTenantId(), request.getTenantId());
    }

    @Test
    public void testWriteToWithNullNodeIds() throws IOException {
        MLDeployModelRequest request = MLDeployModelRequest
            .builder()
            .modelId("modelId")
            .modelNodeIds(null)
            .async(true)
            .dispatchTask(true)
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(VERSION_2_19_0);
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(VERSION_2_19_0);
        MLDeployModelRequest result = new MLDeployModelRequest(in);

        assertEquals(request.getModelId(), result.getModelId());
        assertNull(result.getModelNodeIds());
        assertEquals(request.isAsync(), result.isAsync());
        assertEquals(request.isDispatchTask(), result.isDispatchTask());
    }

    private void parseFromJsonString(String modelId, String expectedInputStr, Consumer<MLDeployModelRequest> verify) throws Exception {
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(
                new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()),
                LoggingDeprecationHandler.INSTANCE,
                expectedInputStr
            );
        parser.nextToken();
        MLDeployModelRequest parsedInput = MLDeployModelRequest.parse(parser, modelId, null);
        verify.accept(parsedInput);
    }
}
