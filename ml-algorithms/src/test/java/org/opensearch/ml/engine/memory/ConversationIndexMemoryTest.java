package org.opensearch.ml.engine.memory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.opensearch.ml.engine.memory.ConversationIndexMemory.APP_TYPE;
import static org.opensearch.ml.engine.memory.ConversationIndexMemory.MEMORY_ID;
import static org.opensearch.ml.engine.memory.ConversationIndexMemory.MEMORY_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.input.execute.agent.ContentBlock;
import org.opensearch.ml.common.input.execute.agent.ContentType;
import org.opensearch.ml.common.input.execute.agent.Message;
import org.opensearch.ml.engine.indices.MLIndicesHandler;
import org.opensearch.ml.memory.action.conversation.CreateConversationResponse;
import org.opensearch.ml.memory.action.conversation.CreateInteractionResponse;
import org.opensearch.transport.client.Client;

public class ConversationIndexMemoryTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Mock
    Client client;

    @Mock
    MLIndicesHandler indicesHandler;

    @Mock
    MLMemoryManager memoryManager;

    ConversationIndexMemory indexMemory;
    ConversationIndexMemory.Factory memoryFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        indexMemory = new ConversationIndexMemory(client, indicesHandler, "test", "test", "test", memoryManager);
        doNothing().when(client).index(any(), any());
        doNothing().when(client).search(any(), any());
        doNothing().when(client).get(any(), any());
        doNothing().when(memoryManager).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        doNothing().when(memoryManager).getFinalInteractions(any(), anyInt(), any());
        doNothing().when(memoryManager).createConversation(any(), any(), any());
        doNothing().when(indicesHandler).initMemoryMetaIndex(any());
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException("test failure"));
            return null;
        }).when(indicesHandler).initMemoryMessageIndex(any());
        memoryFactory = new ConversationIndexMemory.Factory();
        memoryFactory.init(client, indicesHandler, memoryManager);
    }

    @Test
    public void getType() {
        Assert.assertEquals(indexMemory.getType(), ConversationIndexMemory.TYPE);
    }

    @Test
    public void save1() {
        doAnswer(invocation -> {
            ActionListener<CreateInteractionResponse> listener = invocation.getArgument(8);
            listener.onResponse(new CreateInteractionResponse("interaction_id"));
            return null;
        }).when(memoryManager).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        indexMemory.save(new ConversationIndexMessage("test", "123", "question", "response", false), "parent_id", 0, "action");

        verify(memoryManager).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void save6() {
        doAnswer(invocation -> {
            ActionListener<CreateInteractionResponse> listener = invocation.getArgument(8);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(memoryManager).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        indexMemory.save(new ConversationIndexMessage("test", "123", "question", "response", false), "parent_id", 0, "action");

        verify(memoryManager).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void clear() {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("clear method is not supported in ConversationIndexMemory");
        indexMemory.clear();
    }

    @Test
    public void factory_create_emptyMap() {
        ActionListener<ConversationIndexMemory> listener = mock(ActionListener.class);
        memoryFactory.create(Map.of(), listener);

        verify(listener).onFailure(isA(IllegalArgumentException.class));
    }

    @Test
    public void factory_create() {
        ActionListener<ConversationIndexMemory> listener = mock(ActionListener.class);
        memoryFactory.create(Map.of(MEMORY_ID, "123", MEMORY_NAME, "name", APP_TYPE, "app"), listener);

        verify(listener).onResponse(isA(ConversationIndexMemory.class));
    }

    @Test
    public void factory_create_only_memory_id() {
        ActionListener<ConversationIndexMemory> listener = mock(ActionListener.class);
        memoryFactory.create(Map.of(MEMORY_ID, "123"), listener);

        verify(listener).onResponse(isA(ConversationIndexMemory.class));
    }

    @Test
    public void factory_create_empty_memory_id() {
        doAnswer(invocation -> {
            ActionListener<CreateConversationResponse> listener = invocation.getArgument(2);
            listener.onResponse(new CreateConversationResponse("interaction_id"));
            return null;
        }).when(memoryManager).createConversation(any(), any(), any());
        ActionListener<ConversationIndexMemory> listener = mock(ActionListener.class);
        memoryFactory.create(Map.of(MEMORY_NAME, "name", APP_TYPE, "app"), listener);

        verify(listener).onResponse(isA(ConversationIndexMemory.class));
        verify(memoryManager).createConversation(any(), any(), any());
    }

    @Test
    public void factory_create_empty_memory_id_failure() {
        doAnswer(invocation -> {
            ActionListener<CreateConversationResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(memoryManager).createConversation(any(), any(), any());
        ActionListener<ConversationIndexMemory> listener = mock(ActionListener.class);
        memoryFactory.create(Map.of(MEMORY_NAME, "name", APP_TYPE, "app"), listener);

        verify(listener).onFailure(isA(RuntimeException.class));
        verify(memoryManager).createConversation(any(), any(), any());
    }

    // ==================== Tests for saveStructuredMessages ====================
    // saveStructuredMessages is disabled for ConversationIndexMemory (see commit 3405ac54f).
    // All calls should fail with UnsupportedOperationException.

    @Test
    public void saveStructuredMessages_nullMessages() {
        ActionListener<Void> listener = mock(ActionListener.class);
        indexMemory.saveStructuredMessages(null, null, listener);
        verify(listener).onFailure(isA(UnsupportedOperationException.class));
        verify(memoryManager, never()).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void saveStructuredMessages_emptyMessages() {
        ActionListener<Void> listener = mock(ActionListener.class);
        indexMemory.saveStructuredMessages(Collections.emptyList(), null, listener);
        verify(listener).onFailure(isA(UnsupportedOperationException.class));
    }

    @Test
    public void saveStructuredMessages_singlePair_throwsUnsupported() {
        ContentBlock userBlock = new ContentBlock();
        userBlock.setType(ContentType.TEXT);
        userBlock.setText("What is AI?");
        Message userMsg = new Message("user", Collections.singletonList(userBlock));

        ContentBlock assistantBlock = new ContentBlock();
        assistantBlock.setType(ContentType.TEXT);
        assistantBlock.setText("AI is artificial intelligence.");
        Message assistantMsg = new Message("assistant", Collections.singletonList(assistantBlock));

        ActionListener<Void> listener = mock(ActionListener.class);
        indexMemory.saveStructuredMessages(Arrays.asList(userMsg, assistantMsg), null, listener);

        verify(listener).onFailure(isA(UnsupportedOperationException.class));
        verify(memoryManager, never()).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void saveStructuredMessages_multiplePairs_throwsUnsupported() {
        ContentBlock u1 = new ContentBlock();
        u1.setType(ContentType.TEXT);
        u1.setText("Q1");
        ContentBlock a1 = new ContentBlock();
        a1.setType(ContentType.TEXT);
        a1.setText("A1");

        List<Message> messages = Arrays
            .asList(new Message("user", Collections.singletonList(u1)), new Message("assistant", Collections.singletonList(a1)));

        ActionListener<Void> listener = mock(ActionListener.class);
        indexMemory.saveStructuredMessages(messages, null, listener);

        verify(listener).onFailure(isA(UnsupportedOperationException.class));
        verify(memoryManager, never()).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void saveStructuredMessages_onlyUserMessages_throwsUnsupported() {
        ContentBlock block = new ContentBlock();
        block.setType(ContentType.TEXT);
        block.setText("Just a question");
        Message userMsg = new Message("user", Collections.singletonList(block));

        ActionListener<Void> listener = mock(ActionListener.class);
        indexMemory.saveStructuredMessages(Collections.singletonList(userMsg), null, listener);

        verify(listener).onFailure(isA(UnsupportedOperationException.class));
        verify(memoryManager, never()).createInteraction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    // ==================== Tests for getStructuredMessages ====================

    @Test
    public void getStructuredMessages_convertsInteractions() {
        List<org.opensearch.ml.common.conversation.Interaction> interactions = Arrays
            .asList(
                org.opensearch.ml.common.conversation.Interaction.builder().id("i1").input("Question 1").response("Answer 1").build(),
                org.opensearch.ml.common.conversation.Interaction.builder().id("i2").input("Question 2").response("Answer 2").build()
            );

        doAnswer(invocation -> {
            ActionListener<List<org.opensearch.ml.common.conversation.Interaction>> l = invocation.getArgument(2);
            l.onResponse(interactions);
            return null;
        }).when(memoryManager).getFinalInteractions(any(), anyInt(), any());

        ActionListener<List<Message>> listener = mock(ActionListener.class);
        indexMemory.getStructuredMessages(listener);

        org.mockito.ArgumentCaptor<List<Message>> captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        List<Message> result = captor.getValue();
        // 2 interactions * 2 (user + assistant) = 4 messages
        Assert.assertEquals(4, result.size());
        Assert.assertEquals("user", result.get(0).getRole());
        Assert.assertEquals("assistant", result.get(1).getRole());
    }

    @Test
    public void getStructuredMessages_failure() {
        doAnswer(invocation -> {
            ActionListener<?> l = invocation.getArgument(2);
            l.onFailure(new RuntimeException("Retrieval failed"));
            return null;
        }).when(memoryManager).getFinalInteractions(any(), anyInt(), any());

        ActionListener<List<Message>> listener = mock(ActionListener.class);
        indexMemory.getStructuredMessages(listener);

        verify(listener).onFailure(isA(RuntimeException.class));
    }
}
