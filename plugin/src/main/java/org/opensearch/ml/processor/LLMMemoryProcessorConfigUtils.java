/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.processor;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_CONFIG;
import static org.opensearch.ml.processor.MemorySearchResponseProcessor.*;

public class LLMMemoryProcessorConfigUtils {

    private static final String GENERAL_CONVERSATIONAL_MODEL_READ_MEMORY_REQUEST_BODY =
            "{\"role\":\"user\",\"content\":\"${input}\"}, {\"role\":\"system\",\"content\":\"${response}\"},";
    private static final String GENERAL_CONVERSATIONAL_MODEL_SAVE_MEMORY_REQUEST_BODY =
            "{\"input\":\"${ext.ml_inference.llm_question}\", \"response\": \"${llm_answer}\"}";

    public static Map<String, Object> getConfigs(String modelName, String memory_id, Integer message_size) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(READ_ACTION_TYPE, getReadMemoryConfig(modelName, memory_id, message_size));
        configs.put(SAVE_ACTION_TYPE, getSaveMemoryConfig(modelName, memory_id, message_size));
        configs.put(MODEL_CONFIG, getModelSpecificConfig(modelName));
        return configs;
    }

    private static Map<String, Object> getReadMemoryConfig(String modelName, String memory_id, Integer message_size) {
        Map<String, Object> readConfig = new HashMap<>();
        readConfig.put(ACTION_TYPE, READ_ACTION_TYPE);
        readConfig.put(MEMORY_ID, memory_id);
        readConfig.put(MESSAGE_SIZE, message_size);

        switch (modelName.toLowerCase()) {
            case "gpt-4.1-default":
                readConfig.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_READ_MEMORY_REQUEST_BODY);
                break;
            // Add more cases for different models
            default:
                readConfig.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_READ_MEMORY_REQUEST_BODY);
        }

        return readConfig;
    }

    private static Map<String, Object> getSaveMemoryConfig(String modelName, String memory_id, Integer message_size) {
        Map<String, Object> saveConfig = new HashMap<>();
        saveConfig.put(ACTION_TYPE, SAVE_ACTION_TYPE);
        saveConfig.put(MEMORY_ID, memory_id);
        saveConfig.put(MESSAGE_SIZE, message_size);

        switch (modelName.toLowerCase()) {
            case "gpt-4.1-default":
                saveConfig.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_SAVE_MEMORY_REQUEST_BODY);
                break;
            // Add more cases for different models
            default:
                saveConfig.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_SAVE_MEMORY_REQUEST_BODY);
        }

        return saveConfig;
    }

    private static Map<String, Object> getModelSpecificConfig(String modelName) {
        Map<String, Object> modelConfig = new HashMap<>();

        switch (modelName.toLowerCase()) {
            case "gpt-4.1-default":
                modelConfig.put("role", "developer");
                modelConfig.put("prompt", "you are a helpful assistant.");
                modelConfig.put("messages", "[\n" +
                        "                {\"role\":${parameters.role},\"content\":${parameters.prompt} }]},\n" +
                        "                 ${parameters._read_memory} \n" +
                        "                {\"role\":\"user\",\"content\":\"${ext.ml_inference.llm_question}${parameter.context}\" }]}, \n" +
                        "            ]");
                break;
            // Add more cases for different models
            default:
                // Default configuration
                modelConfig.put("role", "developer");
                modelConfig.put("system_prompt", "you are a helpful assistant.");
                modelConfig.put("messages", "[\n" +
                        "                {\"role\":${parameters.role},\"content\":${parameters.prompt} }]},\n" +
                        "                 ${parameters._read_memory} \n" +
                        "                {\"role\":\"user\",\"content\":\"${ext.ml_inference.llm_question}${parameter.context}\" }]}, \n" +
                        "            ]");
        }

        return modelConfig;
    }
}

//package org.opensearch.ml.processor;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.opensearch.ml.processor.InferenceProcessorAttributes.MODEL_CONFIG;
//import static org.opensearch.ml.processor.MemorySearchResponseProcessor.*;
//
//public class LLMMemoryProcessorConfigUtils {
//
//    private static final String GENERAL_CONVERSATIONAL_MODEL_READ_MEMORY_REQUEST_BODY =
//            "{\"role\":\"user\",\"content\":\"${input}\"}, {\"role\":\"system\",\"content\":\"${response}\"},";
//    private static final String GENERAL_CONVERSATIONAL_MODEL_SAVE_MEMORY_REQUEST_BODY =
//            "{\"input\":\"${ext.ml_inference.llm_question}\", \"response\": \"${llm_answer}\"}";
//
//    public Map<String, Object> general_read_memory_config = new HashMap<>();
//
//
//
//    public Map<String, Object> getGeneralReadMemoryConfig(String memory_id, Integer message_size) {
//        general_read_memory_config.put(ACTION_TYPE, READ_ACTION_TYPE);
//        general_read_memory_config.put(MEMORY_ID, memory_id);
//        general_read_memory_config.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_READ_MEMORY_REQUEST_BODY);
//        general_read_memory_config.put(MESSAGE_SIZE, message_size);
//
//        Map<String, String> model_config = new HashMap<>();
//
//        model_config.put("role", "developer");
//        model_config.put("system_prompt", "you are a helpful assistant.");
//        model_config.put("messages", "[\n" +
//                "                {\"role\":${parameters.role},\"content\":${parameters.prompt} }]},\n" +
//                "                 ${parameters._read_memory} \n" +
//                "                {\"role\":\"user\",\"content\":\\\"${ext.ml_inference.llm_question}${parameter.context}\\\" }]}, \n" +
//                "            ]");
//
//        general_read_memory_config.put(MODEL_CONFIG, model_config);
//
//        return general_read_memory_config;
//    }
//
//    public Map<String, Object> getGeneralSaveMemoryConfig(String memory_id, Integer message_size) {
//        general_read_memory_config.put(ACTION_TYPE, SAVE_ACTION_TYPE);
//        general_read_memory_config.put(MEMORY_ID, memory_id);
//        general_read_memory_config.put(REQUEST_BODY, GENERAL_CONVERSATIONAL_MODEL_SAVE_MEMORY_REQUEST_BODY);
//        general_read_memory_config.put(MESSAGE_SIZE, message_size);
//        return general_read_memory_config;
//    }
//
//
//}
