/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.contextmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.ml.common.conversation.Interaction;

/**
 * Unit tests for SlidingWindowManager.
 */
public class SlidingWindowManagerTest {

    private SlidingWindowManager manager;
    private ContextManagerContext context;

    @Before
    public void setUp() {
        manager = new SlidingWindowManager();
        context = ContextManagerContext.builder().chatHistory(new ArrayList<>()).build();
    }

    @Test
    public void testGetType() {
        Assert.assertEquals("SlidingWindowManager", manager.getType());
    }

    @Test
    public void testInitializeWithDefaults() {
        Map<String, Object> config = new HashMap<>();
        manager.initialize(config);

        // Should initialize with default values without throwing exceptions
        Assert.assertNotNull(manager);
    }

    @Test
    public void testInitializeWithCustomConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 10);

        manager.initialize(config);

        // Should initialize without throwing exceptions
        Assert.assertNotNull(manager);
    }

    @Test
    public void testInitializeWithActivationRules() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> activation = new HashMap<>();
        activation.put("message_count_exceed", 15);
        config.put("activation", activation);

        manager.initialize(config);

        // Should initialize without throwing exceptions
        Assert.assertNotNull(manager);
    }

    @Test
    public void testInitializeWithInvalidMaxMessages() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", -5);

        // Should handle invalid config gracefully and use default
        manager.initialize(config);

        Assert.assertNotNull(manager);
    }

    @Test
    public void testShouldActivateWithNoRules() {
        Map<String, Object> config = new HashMap<>();
        manager.initialize(config);

        // Should always activate when no rules are defined
        Assert.assertTrue(manager.shouldActivate(context));
    }

    @Test
    public void testShouldActivateWithMessageCountRule() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> activation = new HashMap<>();
        activation.put("message_count_exceed", 5);
        config.put("activation", activation);

        manager.initialize(config);

        // Create context with few messages (should not activate)
        addInteractionsToContext(3);
        Assert.assertFalse(manager.shouldActivate(context));

        // Add more messages (should activate)
        addInteractionsToContext(5);
        Assert.assertTrue(manager.shouldActivate(context));
    }

    @Test
    public void testExecuteWithEmptyChatHistory() {
        Map<String, Object> config = new HashMap<>();
        manager.initialize(config);

        // Should handle empty chat history gracefully
        manager.execute(context);

        Assert.assertTrue(context.getChatHistory().isEmpty());
    }

    @Test
    public void testExecuteWithSmallChatHistory() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 10);
        manager.initialize(config);

        // Add fewer messages than the limit
        addInteractionsToContext(5);
        int originalSize = context.getChatHistory().size();

        manager.execute(context);

        // Chat history should remain unchanged
        Assert.assertEquals(originalSize, context.getChatHistory().size());
    }

    @Test
    public void testExecuteWithLargeChatHistory() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 5);
        manager.initialize(config);

        // Add more messages than the limit
        addInteractionsToContext(10);

        manager.execute(context);

        // Chat history should be truncated to the limit
        Assert.assertEquals(5, context.getChatHistory().size());

        // Should keep the most recent messages
        List<Interaction> chatHistory = context.getChatHistory();
        for (int i = 0; i < chatHistory.size(); i++) {
            Interaction interaction = chatHistory.get(i);
            // The kept messages should be from the end of the original list
            String expectedInput = "User input " + (6 + i); // Messages 6-10
            Assert.assertEquals(expectedInput, interaction.getInput());
        }
    }

    @Test
    public void testExecuteKeepsMostRecentMessages() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 3);
        manager.initialize(config);

        // Add messages with identifiable content
        for (int i = 1; i <= 7; i++) {
            Interaction interaction = Interaction.builder().input("Message " + i).response("Response " + i).build();
            context.getChatHistory().add(interaction);
        }

        manager.execute(context);

        // Should keep the last 3 messages (5, 6, 7)
        Assert.assertEquals(3, context.getChatHistory().size());

        List<Interaction> chatHistory = context.getChatHistory();
        Assert.assertEquals("Message 5", chatHistory.get(0).getInput());
        Assert.assertEquals("Message 6", chatHistory.get(1).getInput());
        Assert.assertEquals("Message 7", chatHistory.get(2).getInput());
    }

    @Test
    public void testExecuteWithExactLimit() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 5);
        manager.initialize(config);

        // Add exactly the limit number of messages
        addInteractionsToContext(5);
        int originalSize = context.getChatHistory().size();

        manager.execute(context);

        // Chat history should remain unchanged
        Assert.assertEquals(originalSize, context.getChatHistory().size());
    }

    @Test
    public void testExecuteWithNullChatHistory() {
        Map<String, Object> config = new HashMap<>();
        manager.initialize(config);

        context.setChatHistory(null);

        // Should handle null chat history gracefully
        manager.execute(context);

        // Should not throw exception
        Assert.assertNull(context.getChatHistory());
    }

    @Test
    public void testExecuteWithDifferentMessageTypes() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", 3);
        manager.initialize(config);

        // Add different types of interactions
        Interaction userMessage = Interaction.builder().input("User question").build();

        Interaction assistantMessage = Interaction.builder().response("Assistant response").build();

        Interaction fullInteraction = Interaction.builder().input("User input").response("Assistant response").build();

        // Add more than the limit
        context.getChatHistory().add(userMessage);
        context.getChatHistory().add(assistantMessage);
        context.getChatHistory().add(fullInteraction);
        context.getChatHistory().add(userMessage);
        context.getChatHistory().add(assistantMessage);

        manager.execute(context);

        // Should keep the last 3 messages regardless of type
        Assert.assertEquals(3, context.getChatHistory().size());
    }

    @Test
    public void testInvalidMaxMessagesConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("max_messages", "invalid_number");

        // Should handle invalid config gracefully and use default
        manager.initialize(config);

        Assert.assertNotNull(manager);
    }

    /**
     * Helper method to add interactions to the context.
     */
    private void addInteractionsToContext(int count) {
        for (int i = 1; i <= count; i++) {
            Interaction interaction = Interaction.builder().input("User input " + i).response("Assistant response " + i).build();
            context.getChatHistory().add(interaction);
        }
    }
}
