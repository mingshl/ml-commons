package org.opensearch.ml.engine.agents;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.contextmanager.ContextManagerContext;
import org.opensearch.ml.common.conversation.Interaction;
import org.opensearch.ml.common.hooks.HookRegistry;
import org.opensearch.ml.common.memory.Memory;

public class AgentContextUtilTest {

    @Test
    public void testEmitPreLLMHookWithNullHookRegistry() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", "test question");

        List<String> interactions = new ArrayList<>();
        List<MLToolSpec> toolSpecs = new ArrayList<>();
        Memory memory = mock(Memory.class);

        ContextManagerContext result = AgentContextUtil.emitPreLLMHook(parameters, interactions, toolSpecs, memory, null);

        assertNotNull(result);
        assertEquals("test question", result.getUserPrompt());
    }

    @Test
    public void testEmitPreLLMHookWithValidHookRegistry() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", "test question");

        List<String> interactions = new ArrayList<>();
        List<MLToolSpec> toolSpecs = new ArrayList<>();
        Memory memory = mock(Memory.class);
        HookRegistry hookRegistry = mock(HookRegistry.class);

        ContextManagerContext result = AgentContextUtil.emitPreLLMHook(parameters, interactions, toolSpecs, memory, hookRegistry);

        assertNotNull(result);
        assertEquals("test question", result.getUserPrompt());
        verify(hookRegistry, times(1)).emit(any());
    }

    @Test
    public void testEmitPostMemoryHookWithNullHookRegistry() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", "test question");

        List<Interaction> retrievedHistory = new ArrayList<>();
        retrievedHistory.add(Interaction.builder().id("1").input("q1").response("r1").build());
        List<MLToolSpec> toolSpecs = new ArrayList<>();
        Memory memory = mock(Memory.class);

        ContextManagerContext result = AgentContextUtil.emitPostMemoryHook(parameters, retrievedHistory, toolSpecs, memory, null);

        assertNotNull(result);
        assertEquals("test question", result.getUserPrompt());
        assertEquals(1, result.getChatHistory().size());
        assertEquals("q1", result.getChatHistory().get(0).getInput());
    }

    @Test
    public void testEmitPostMemoryHookWithValidHookRegistry() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", "test question");

        List<Interaction> retrievedHistory = new ArrayList<>();
        retrievedHistory.add(Interaction.builder().id("1").input("q1").response("r1").build());
        retrievedHistory.add(Interaction.builder().id("2").input("q2").response("r2").build());
        List<MLToolSpec> toolSpecs = new ArrayList<>();
        Memory memory = mock(Memory.class);
        HookRegistry hookRegistry = mock(HookRegistry.class);

        ContextManagerContext result = AgentContextUtil.emitPostMemoryHook(
            parameters, retrievedHistory, toolSpecs, memory, hookRegistry
        );

        assertNotNull(result);
        assertEquals("test question", result.getUserPrompt());
        assertEquals(2, result.getChatHistory().size());
        verify(hookRegistry, times(1)).emit(any());
    }

    @Test
    public void testEmitPostMemoryHookWithEmptyHistory() {
        Map<String, String> parameters = new HashMap<>();
        Memory memory = mock(Memory.class);
        HookRegistry hookRegistry = mock(HookRegistry.class);

        ContextManagerContext result = AgentContextUtil.emitPostMemoryHook(
            parameters, new ArrayList<>(), null, memory, hookRegistry
        );

        assertNotNull(result);
        assertTrue(result.getChatHistory().isEmpty());
        verify(hookRegistry, times(1)).emit(any());
    }

    @Test
    public void testEmitPostMemoryHookWithNullHistory() {
        Map<String, String> parameters = new HashMap<>();
        Memory memory = mock(Memory.class);
        HookRegistry hookRegistry = mock(HookRegistry.class);

        ContextManagerContext result = AgentContextUtil.emitPostMemoryHook(
            parameters, null, null, memory, hookRegistry
        );

        assertNotNull(result);
        assertTrue(result.getChatHistory().isEmpty());
        verify(hookRegistry, times(1)).emit(any());
    }

    @Test
    public void testEmitPostMemoryHookWithException() {
        Map<String, String> parameters = new HashMap<>();
        List<Interaction> retrievedHistory = new ArrayList<>();
        retrievedHistory.add(Interaction.builder().id("1").input("q1").response("r1").build());
        Memory memory = mock(Memory.class);
        HookRegistry hookRegistry = mock(HookRegistry.class);
        doThrow(new RuntimeException("hook error")).when(hookRegistry).emit(any());

        ContextManagerContext result = AgentContextUtil.emitPostMemoryHook(
            parameters, retrievedHistory, null, memory, hookRegistry
        );

        // Should return context without throwing, fallback behavior
        assertNotNull(result);
        assertEquals(1, result.getChatHistory().size());
    }

    @Test
    public void testBuildContextManagerContextForMemory() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("system_prompt", "You are a helpful assistant");
        parameters.put("question", "What is OpenSearch?");

        List<Interaction> history = new ArrayList<>();
        history.add(Interaction.builder().id("1").input("hello").response("hi").build());
        history.add(Interaction.builder().id("2").input("how are you").response("good").build());

        List<MLToolSpec> toolSpecs = new ArrayList<>();
        toolSpecs.add(MLToolSpec.builder().name("tool1").type("type1").build());

        ContextManagerContext result = AgentContextUtil.buildContextManagerContextForMemory(
            parameters, history, toolSpecs, null
        );

        assertNotNull(result);
        assertEquals("You are a helpful assistant", result.getSystemPrompt());
        assertEquals("What is OpenSearch?", result.getUserPrompt());
        assertEquals(2, result.getChatHistory().size());
        assertEquals("hello", result.getChatHistory().get(0).getInput());
        assertEquals("how are you", result.getChatHistory().get(1).getInput());
        assertEquals(1, result.getToolConfigs().size());
    }
}
