package org.opensearch.ml.processor;
import com.jayway.jsonpath.internal.function.ParamType;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.searchpipelines.questionanswering.generative.client.ConversationalMemoryClient;
import org.opensearch.transport.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.ml.common.conversation.Interaction;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.xcontent.XContentType;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.ml.common.utils.StringUtils.isJson;

@Log4j2
public class MemorySearchResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    private static final Logger logger = LogManager.getLogger(MemorySearchResponseProcessor.class);

    public static final String TYPE = "memory";
    public static final String ACTION_TYPE = "action_type";
    public static final String READ_ACTION_TYPE = "read";
    public static final String SAVE_ACTION_TYPE = "save";
    public static final String MEMORY_ID = "memory_id";
    public static final String REQUEST_BODY = "request_body";
    public static final String MESSAGE_SIZE = "message_size";
    private static final int DEFAULT_MESSAGE_SIZE = 10;

    private final String actionType;
    private final String memoryId;
    private final String requestBody;
    private final int messageSize;
    private final Client client;
    private final ConversationalMemoryClient memoryClient;
    private final boolean ignoreFailure;

    //TODO do we need memory_config fields? TODO to pass over prompt

    protected MemorySearchResponseProcessor(
            String tag,
            String description,
            boolean ignoreFailure,
            String actionType,
            String memoryId,
            String requestBody,
            int messageSize,
            Client client,
            ConversationalMemoryClient memoryClient) {
        super(tag, description, ignoreFailure);
        this.actionType = actionType;
        this.memoryId = memoryId;
        this.requestBody = requestBody;
        this.messageSize = messageSize;
        this.client = client;
        this.memoryClient = memoryClient;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        throw new RuntimeException("Memory search response processor makes asynchronous calls and does not call processResponse");
    }

    @Override
    public void processResponseAsync(
            SearchRequest request,
            SearchResponse response,
            PipelineProcessingContext responseContext,
            ActionListener<SearchResponse> responseListener
    ) {
        try {
            if ("read".equalsIgnoreCase(actionType)) {
                readMemory(response, responseContext, responseListener);
            } else if ("save".equalsIgnoreCase(actionType)) {
                saveMemory(response, responseContext, responseListener);
            } else {
                throw new IllegalArgumentException("Unsupported action type: " + actionType);
            }
        } catch (Exception e) {
            if (ignoreFailure) {
                logger.error("Failed to process memory operation", e);
                responseListener.onResponse(response);
            } else {
                responseListener.onFailure(e);
            }
        }
    }

    private void readMemory(SearchResponse response, PipelineProcessingContext responseContext, ActionListener<SearchResponse> responseListener) {
        final Instant memoryStart = Instant.now();

        if (memoryId == null || memoryId.isEmpty()) {
            String newMemoryId = memoryClient.createConversation("new_conversation");
            logger.debug("Created new conversation with ID: {} ({})", newMemoryId, getDuration(memoryStart));
            responseListener.onResponse(response);
        } else {
            memoryClient.getInteractions(memoryId, messageSize, new ActionListener<List<Interaction>>() {
                @Override
                public void onResponse(List<Interaction> interactions) {
                    logger.debug("Retrieved {} interactions from memory {} ({})",
                            interactions.size(), memoryId, getDuration(memoryStart));
                    // Store interactions in response context for use by subsequent processors
                    responseContext.setAttribute("_interactions", interactions);
                    // Store the _read_memory attribute in the responseContext
                    responseContext.setAttribute("_read_memory", parseRequestBodyFromInteractions(requestBody, interactions));
                    responseListener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(e, response, responseListener);
                }
            });
        }
    }

    private void saveMemory(SearchResponse response, PipelineProcessingContext responseContext, ActionListener<SearchResponse> responseListener) {
        //TODO generate a new memory_id is not provided
        if (memoryId == null || memoryId.isEmpty()) {
            responseListener.onFailure(new IllegalArgumentException("Memory ID is required for save operation"));
            return;
        }

        Map<String, Object> processorContextAttributes = responseContext.getAttributes();

        final Instant memoryStart = Instant.now();
        Map<String, Object> messageParams = parseRequestBodyToMap(requestBody,processorContextAttributes);

        memoryClient.createInteraction(
                memoryId,
                (String) messageParams.get("input"),
                (String) messageParams.get("prompt_template"),
                (String) messageParams.get("response"),
                TYPE,
                (Map<String, String>) messageParams.get("additional_info"),
                new ActionListener<String>() {
                    @Override
                    public void onResponse(String interactionId) {
                        logger.debug("Created new interaction: {} ({}) for memory {}",
                                interactionId, getDuration(memoryStart), memoryId);
                        responseListener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleFailure(e, response, responseListener);
                    }
                }
        );
    }

    private void handleFailure(Exception e, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            logger.error("Failed to process memory operation", e);
            responseListener.onResponse(response);
        } else {
            responseListener.onFailure(e);
        }
    }

    private long getDuration(Instant start) {
        return Duration.between(start, Instant.now()).toMillis();
    }

    /**
     * Parses request body by substituting parameters from a list of interactions
     * @param requestBody The template request body containing placeholders
     * @param interactions List of interactions to extract parameters from
     * @return Concatenated string of processed request bodies
     */
    private String parseRequestBodyFromInteractions(String requestBody, List<Interaction> interactions) {
        if (requestBody == null || interactions == null || interactions.isEmpty()) {
            return "";
        }

        StringBuilder resultBuilder = new StringBuilder();

        for (Interaction interaction : interactions) {
            Map<String, Object> parameters = interaction.toMap();
            if (!parameters.isEmpty()) {
                // Convert Object values to String values for the substitutor
                // TODO need to handle the fields within additional_info and remove the prefix
                Map<String, String> stringParameters = parameters.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue() != null ? StringUtils.toJson(e.getValue()) : ""
                        ));

                StringSubstitutor parametersSubstitutor = new StringSubstitutor(stringParameters, "${", "}");
                String processedPayload = parametersSubstitutor.replace(requestBody);
                if (resultBuilder.length() > 0) {
                    resultBuilder.append(" ");
                }
                resultBuilder.append(processedPayload);
            }
        }

        return resultBuilder.toString();
    }


    private Map<String, Object> parseRequestBodyToMap(String requestBody, Map<String, Object> processorContextAttributes) {
        Map<String, String> stringParameters = processorContextAttributes.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue() != null ? StringUtils.toJson(e.getValue()) : ""
                ));
        StringSubstitutor parametersSubstitutor = new StringSubstitutor(stringParameters, "${", "}");
        String processedPayload = parametersSubstitutor.replace(requestBody);

        if (!isJson(processedPayload)) {
            throw new IllegalArgumentException("Json format is expected. Invalid payload: " + processedPayload);
        }

        Map<String, Object> requestBodyMap;
        try {
            XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, processedPayload);

            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            requestBodyMap = parser.map();

        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse request body during save memory", e);
        }
        return requestBodyMap;
    }

    @Override
    public String getType() {
        return TYPE;
    }


    public static class Factory implements Processor.Factory<SearchResponseProcessor> {
        private final Client client;

        public Factory(Client client) {
            this.client = client;
        }

        @Override
        public MemorySearchResponseProcessor create(
                Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
                String tag,
                String description,
                boolean ignoreFailure,
                Map<String, Object> config,
                PipelineContext pipelineContext
        ) {
            String actionType = ConfigurationUtils.readStringProperty(TYPE, tag, config, ACTION_TYPE);
            String memoryId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MEMORY_ID, null);
            String requestBody = ConfigurationUtils.readStringProperty(TYPE, tag, config, REQUEST_BODY, null);
            int messageSize = ConfigurationUtils.readIntProperty(TYPE, tag, config, MESSAGE_SIZE, DEFAULT_MESSAGE_SIZE);
            // Create memory client here for testing purposes
            // TO move
            ConversationalMemoryClient memoryClient = new ConversationalMemoryClient(client);
            return new MemorySearchResponseProcessor(
                    tag,
                    description,
                    ignoreFailure,
                    actionType,
                    memoryId,
                    requestBody,
                    messageSize,
                    client,
                    memoryClient
            );
        }
    }
}