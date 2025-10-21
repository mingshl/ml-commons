/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.contextmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.ml.common.conversation.Interaction;

import lombok.extern.log4j.Log4j2;

/**
 * Context manager that implements a sliding window approach for chat history management.
 * Keeps only the most recent N messages in the chat history to prevent context window overflow.
 * This manager ensures proper handling of different message types while maintaining conversation flow.
 */
@Log4j2
public class SlidingWindowManager implements ContextManager {

    public static final String TYPE = "SlidingWindowManager";

    // Configuration keys
    private static final String MAX_MESSAGES_KEY = "max_messages";

    // Default values
    private static final int DEFAULT_MAX_MESSAGES = 20;

    private int maxMessages;
    private List<ActivationRule> activationRules;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void initialize(Map<String, Object> config) {
        // Initialize configuration with defaults
        this.maxMessages = parseIntegerConfig(config, MAX_MESSAGES_KEY, DEFAULT_MAX_MESSAGES);

        if (this.maxMessages <= 0) {
            log.warn("Invalid max_messages value: {}, using default {}", this.maxMessages, DEFAULT_MAX_MESSAGES);
            this.maxMessages = DEFAULT_MAX_MESSAGES;
        }

        // Initialize activation rules from config
        @SuppressWarnings("unchecked")
        Map<String, Object> activationConfig = (Map<String, Object>) config.get("activation");
        this.activationRules = ActivationRuleFactory.createRules(activationConfig);

        log.info("Initialized SlidingWindowManager: maxMessages={}", maxMessages);
    }

    @Override
    public boolean shouldActivate(ContextManagerContext context) {
        if (activationRules == null || activationRules.isEmpty()) {
            // No activation rules means always activate
            return true;
        }

        // All activation rules must be satisfied (AND logic)
        for (ActivationRule rule : activationRules) {
            if (!rule.evaluate(context)) {
                log.debug("Activation rule not satisfied: {}", rule.getDescription());
                return false;
            }
        }

        log.debug("All activation rules satisfied, manager will execute");
        return true;
    }

    @Override
    public void execute(ContextManagerContext context) {
        List<Interaction> chatHistory = context.getChatHistory();

        if (chatHistory == null || chatHistory.isEmpty()) {
            log.debug("No chat history to process");
            return;
        }

        int originalSize = chatHistory.size();

        if (originalSize <= maxMessages) {
            log.debug("Chat history size ({}) is within limit ({}), no truncation needed", originalSize, maxMessages);
            return;
        }

        // Keep the most recent messages
        List<Interaction> truncatedHistory = new ArrayList<>(chatHistory.subList(originalSize - maxMessages, originalSize));

        context.setChatHistory(truncatedHistory);

        int removedMessages = originalSize - maxMessages;
        log.info("Applied sliding window: kept {} most recent messages, removed {} older messages", maxMessages, removedMessages);

        // Log some details about what was kept/removed for debugging
        if (log.isDebugEnabled()) {
            log
                .debug(
                    "Sliding window applied - Original size: {}, New size: {}, Removed: {}",
                    originalSize,
                    truncatedHistory.size(),
                    removedMessages
                );

            if (!truncatedHistory.isEmpty()) {
                Interaction firstKept = truncatedHistory.get(0);
                log
                    .debug(
                        "First kept interaction: {}",
                        firstKept.getInput() != null
                            ? firstKept.getInput().substring(0, Math.min(50, firstKept.getInput().length())) + "..."
                            : "null"
                    );
            }
        }
    }

    /**
     * Parse an integer configuration value with a default fallback.
     */
    private int parseIntegerConfig(Map<String, Object> config, String key, int defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            } else if (value instanceof String) {
                return Integer.parseInt((String) value);
            } else {
                log.warn("Invalid type for config key '{}': {}, using default {}", key, value.getClass().getSimpleName(), defaultValue);
                return defaultValue;
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid integer value for config key '{}': {}, using default {}", key, value, defaultValue);
            return defaultValue;
        }
    }
}
