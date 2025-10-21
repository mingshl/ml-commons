/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.contextmanager;

import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

/**
 * Context manager that truncates tool outputs when they exceed token limits.
 * This manager helps prevent LLM token limit errors by reducing the size of tool outputs
 * while preserving the most important information based on the configured truncation strategy.
 */
@Log4j2
public class ToolsOutputTruncateManager implements ContextManager {

    public static final String TYPE = "ToolsOutputTruncateManager";

    // Configuration keys
    private static final String MAX_TOKENS_KEY = "max_tokens";
    private static final String TRUNCATION_STRATEGY_KEY = "truncation_strategy";
    private static final String TRUNCATION_MARKER_KEY = "truncation_marker";

    // Truncation strategies
    private static final String PRESERVE_BEGINNING = "preserve_beginning";
    private static final String PRESERVE_END = "preserve_end";
    private static final String PRESERVE_MIDDLE = "preserve_middle";

    // Default values
    private static final int DEFAULT_MAX_TOKENS = 35000;
    private static final String DEFAULT_TRUNCATION_STRATEGY = PRESERVE_BEGINNING;
    private static final String DEFAULT_TRUNCATION_MARKER = "... [Content truncated due to length]";

    private int maxTokens;
    private String truncationStrategy;
    private String truncationMarker;
    private TokenCounter tokenCounter;
    private List<ActivationRule> activationRules;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void initialize(Map<String, Object> config) {
        // Initialize configuration with defaults
        this.maxTokens = parseIntegerConfig(config, MAX_TOKENS_KEY, DEFAULT_MAX_TOKENS);
        this.truncationStrategy = parseStringConfig(config, TRUNCATION_STRATEGY_KEY, DEFAULT_TRUNCATION_STRATEGY);
        this.truncationMarker = parseStringConfig(config, TRUNCATION_MARKER_KEY, DEFAULT_TRUNCATION_MARKER);

        // Validate truncation strategy
        if (!isValidTruncationStrategy(truncationStrategy)) {
            log.warn("Invalid truncation strategy '{}', using default '{}'", truncationStrategy, DEFAULT_TRUNCATION_STRATEGY);
            this.truncationStrategy = DEFAULT_TRUNCATION_STRATEGY;
        }

        // Initialize token counter (using character-based as fallback)
        this.tokenCounter = new CharacterBasedTokenCounter();

        // Initialize activation rules from config
        @SuppressWarnings("unchecked")
        Map<String, Object> activationConfig = (Map<String, Object>) config.get("activation");
        this.activationRules = ActivationRuleFactory.createRules(activationConfig);

        log
            .info(
                "Initialized ToolsOutputTruncateManager: maxTokens={}, strategy={}, marker='{}'",
                maxTokens,
                truncationStrategy,
                truncationMarker
            );
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
        List<Map<String, Object>> toolInteractions = context.getToolInteractions();

        if (toolInteractions == null || toolInteractions.isEmpty()) {
            log.debug("No tool interactions to process");
            return;
        }

        int truncatedCount = 0;
        int totalTokensSaved = 0;

        for (Map<String, Object> interaction : toolInteractions) {
            Object output = interaction.get("output");
            if (output instanceof String) {
                String outputStr = (String) output;
                int originalTokenCount = tokenCounter.count(outputStr);

                if (originalTokenCount > maxTokens) {
                    String truncatedOutput = truncateOutput(outputStr, maxTokens);
                    String finalOutput = truncatedOutput + truncationMarker;

                    interaction.put("output", finalOutput);

                    int newTokenCount = tokenCounter.count(finalOutput);
                    int tokensSaved = originalTokenCount - newTokenCount;
                    totalTokensSaved += tokensSaved;
                    truncatedCount++;

                    log
                        .info(
                            "Truncated tool output: {} tokens -> {} tokens (saved {} tokens)",
                            originalTokenCount,
                            newTokenCount,
                            tokensSaved
                        );
                }
            }
        }

        if (truncatedCount > 0) {
            log.info("ToolsOutputTruncateManager processed {} tool outputs, saved {} tokens total", truncatedCount, totalTokensSaved);
        } else {
            log.debug("No tool outputs required truncation");
        }
    }

    /**
     * Truncate the output text based on the configured strategy.
     * @param output the original output text
     * @param maxTokens the maximum number of tokens to keep
     * @return the truncated text (without the truncation marker)
     */
    private String truncateOutput(String output, int maxTokens) {
        // Reserve some tokens for the truncation marker
        int markerTokens = tokenCounter.count(truncationMarker);
        int availableTokens = Math.max(1, maxTokens - markerTokens);

        switch (truncationStrategy) {
            case PRESERVE_BEGINNING:
                return tokenCounter.truncateFromEnd(output, availableTokens);
            case PRESERVE_END:
                return tokenCounter.truncateFromBeginning(output, availableTokens);
            case PRESERVE_MIDDLE:
                return tokenCounter.truncateMiddle(output, availableTokens);
            default:
                log.warn("Unknown truncation strategy '{}', using preserve_beginning", truncationStrategy);
                return tokenCounter.truncateFromEnd(output, availableTokens);
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

    /**
     * Parse a string configuration value with a default fallback.
     */
    private String parseStringConfig(Map<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof String) {
            return (String) value;
        } else {
            log.warn("Invalid type for config key '{}': {}, using default '{}'", key, value.getClass().getSimpleName(), defaultValue);
            return defaultValue;
        }
    }

    /**
     * Check if the truncation strategy is valid.
     */
    private boolean isValidTruncationStrategy(String strategy) {
        return PRESERVE_BEGINNING.equals(strategy) || PRESERVE_END.equals(strategy) || PRESERVE_MIDDLE.equals(strategy);
    }
}
