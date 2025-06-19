/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.processor;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.ml.common.conversation.Interaction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.searchpipelines.questionanswering.generative.client.ConversationalMemoryClient;
import org.opensearch.transport.client.Client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MemorySearchResponseProcessorTests {

    @Mock
    private Client client;

    @Mock
    private SearchResponse searchResponse;

    @Mock
    private SearchRequest searchRequest;

    @Mock
    private PipelineProcessingContext processingContext;

    @Mock
    private ConversationalMemoryClient memoryClient;

    private MemorySearchResponseProcessor processor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testReadMemorySuccess() {
        // Create processor with read action
        processor = new MemorySearchResponseProcessor(
                "test_tag",
                "test_description",
                false,
                "read",
                "test_memory_id",
                "{\"role\":\"user\",\"content\":\"${input}\"}, {\"role\":\"system\",\"content\":\"${response}\"},",
                5,
                client,
                memoryClient);

        // Create sample interactions
        List<Interaction> sampleInteractions = createSampleInteractions();

        // Mock memory client behavior
        doAnswer(invocation -> {
            ActionListener<List<Interaction>> listener = invocation.getArgument(2);
            listener.onResponse(sampleInteractions);
            return null;
        }).when(memoryClient).getInteractions(eq("test_memory_id"), eq(5), any());

        // Create response listener

        ActionListener<SearchResponse> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse newSearchResponse) {
                assertNotNull(newSearchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("error handling not properly");
            }

        };

         // Execute processor
        processor.processResponseAsync(searchRequest, searchResponse, processingContext, responseListener);

        InOrder inOrder = inOrder(processingContext);
        inOrder.verify(processingContext).setAttribute("_interactions", sampleInteractions);
        String expectedReadMemoryString =
                "{\"role\":\"user\",\"content\":\"What is OpenSearch\"}, " +
                        "{\"role\":\"system\",\"content\":\"OpenSearch is an open source search and analytics suite.\"}, " +
                        "{\"role\":\"user\",\"content\":\"How to use OpenSearch?\"}, " +
                        "{\"role\":\"system\",\"content\":\"You can use OpenSearch by...\"},";
        inOrder.verify(processingContext).setAttribute("_read_memory", expectedReadMemoryString);
        System.out.print("Interactions stored in context: " + processingContext+ "\n");
        System.out.print("Interactions: " + sampleInteractions+ "\n");
    }


    @Test
    public void testReadMemoryFailure() {
        // Create processor with read action
        processor = new MemorySearchResponseProcessor(
                "test_tag",
                "test_description",
                false,
                "read",
                "test_memory_id",
                null,
                5,
                client,
                memoryClient);

        // Mock memory client to return error
        RuntimeException testException = new RuntimeException("Test error");
        doAnswer(invocation -> {
            ActionListener<List<Interaction>> listener = invocation.getArgument(2);
            listener.onFailure(testException);
            return null;
        }).when(memoryClient).getInteractions(eq("test_memory_id"), eq(5), any());

        // Create response listener
        ActionListener<SearchResponse> responseListener = mock(ActionListener.class);

        // Execute processor
        processor.processResponseAsync(searchRequest, searchResponse, processingContext, responseListener);

        // Verify error handling
        verify(responseListener).onFailure(testException);
    }

    @Test
    public void testSaveMemorySuccess() {
        // Create processor with save action
        processor = new MemorySearchResponseProcessor(
                "test_tag",
                "test_description",
                false,
                "save",
                "test_memory_id",
                "{\"input\":\"${ext.ml_inference.llm_question}\", \"response\": \"${llm_answer}\"}",
                5,
                client,
                memoryClient);

        // Create response listener
        ActionListener<SearchResponse> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse newSearchResponse) {
                assertNotNull(newSearchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("error handling not properly: " + e.getMessage());
            }

        };
        ActionListener<String> memoryListener = new ActionListener<String>() {
            @Override
            public void onResponse(String interactionId) {
                responseListener.onResponse(searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("error handling not properly: " + e.getMessage());
            }
        };
        // Mock memory client behavior
        doAnswer(invocation -> {
            ActionListener<String> listener = invocation.getArgument(5);
            listener.onResponse("test_interaction_id");
            return null;
        }).when(memoryClient).createInteraction(
                eq("test_memory_id"),
                eq("when is the next OpenSearch release?"),
                isNull(),
                eq("June 2025"),
                eq("memory"),
                isNull(),
                eq(memoryListener)
        );

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("ext.ml_inference.llm_question", "when is the next OpenSearch release?" );
        attributes.put("llm_answer", "June 2025" );
        when(processingContext.getAttributes()).thenReturn(attributes);

        // Execute processor
        processor.processResponseAsync(searchRequest, searchResponse, processingContext, responseListener);

        verify(memoryClient).createInteraction(
                eq("test_memory_id"),
                eq("when is the next OpenSearch release?"),
                isNull(),
                eq("June 2025"),
                eq("memory"),
                any(),
                any(ActionListener.class)
        );
    }


    private List<Interaction> createSampleInteractions() {
        List<Interaction> interactions = new ArrayList<>();
        Instant time1 = Instant.now();
        Instant time2 = time1.plusSeconds(60); // Second interaction 1 minute later
        // Create sample interaction 1
        Interaction interaction1 = Interaction
                .builder()
                .id("test_memory_id")
                .createTime(time1)
                .conversationId("interaction_id_1")
                .input("What is OpenSearch")
                .promptTemplate("test_prompt_template")
                .response("OpenSearch is an open source search and analytics suite.")
                .origin("memory_processor")
                .additionalInfo(Collections.singletonMap("context", "test context"))
                .parentInteractionId("parent_id1")
                .traceNum(1)
                .build();

        // Create sample interaction 2
        Interaction interaction2 =  Interaction
                .builder()
                .id("test_memory_id")
                .createTime(time2)
                .conversationId("interaction_id_2")
                .input("How to use OpenSearch?")
                .promptTemplate("test_prompt_template")
                .response("You can use OpenSearch by...")
                .origin("memory_processor")
                .additionalInfo(Collections.singletonMap("suggestion", "new suggestion"))
                .parentInteractionId("interaction_id_1")
                .traceNum(1)
                .build();


        interactions.add(interaction1);
        interactions.add(interaction2);
        return interactions;
    }

//    private SearchResponse getSearchResponse(int size, boolean includeMapping, String fieldName) {
//        SearchHit[] hits = new SearchHit[size];
//        for (int i = 0; i < size; i++) {
//            Map<String, DocumentField> searchHitFields = new HashMap<>();
//            if (includeMapping) {
//                searchHitFields.put(fieldName, new DocumentField("value " + i, Collections.emptyList()));
//            }
//            searchHitFields.put(fieldName, new DocumentField("value " + i, Collections.emptyList()));
//            hits[i] = new SearchHit(i, "doc " + i, searchHitFields, Collections.emptyMap());
//            hits[i].sourceRef(new BytesArray("{ \"" + fieldName + "\" : \"value " + i + "\" }"));
//            hits[i].score(i);
//        }
//        SearchHits searchHits = new SearchHits(hits, new TotalHits(size * 2L, TotalHits.Relation.EQUAL_TO), size);
//        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
//        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
//    }
}