package org.example.livelunchchoice.vote.messaging.local;

import org.example.livelunchchoice.common.error.GlobalExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class LocalConsumerControllerTests {
    private KafkaListenerEndpointRegistry registry;
    private MessageListenerContainer container;
    private MockMvc mvc;

    @BeforeEach
    void setup() {
        registry = mock(KafkaListenerEndpointRegistry.class);
        container = mock(MessageListenerContainer.class);
        when(registry.getListenerContainer("vote-log-listener")).thenReturn(container);
        mvc = MockMvcBuilders.standaloneSetup(new LocalConsumerController(registry))
                .setControllerAdvice(new GlobalExceptionHandler()).build();
    }

    @Test
    void stopsUsingCompletionCallback() throws Exception {
        when(container.isRunning()).thenReturn(true);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            callback.run();
            return null;
        }).when(container).stop(any(Runnable.class));
        mvc.perform(post("/api/local/kafka/consumer/stop")).andExpect(status().isNoContent());
        verify(container).stop(any(Runnable.class));
        verify(container, never()).start();
    }

    @Test
    void startsStoppedContainer() throws Exception {
        mvc.perform(post("/api/local/kafka/consumer/start")).andExpect(status().isNoContent());
        verify(container).start();
    }

    @Test
    void repeatedCommandsDoNotRestartOrStopAgain() throws Exception {
        when(container.isRunning()).thenReturn(true);
        mvc.perform(post("/api/local/kafka/consumer/start")).andExpect(status().isNoContent());
        verify(container, never()).start();
        when(container.isRunning()).thenReturn(false);
        mvc.perform(post("/api/local/kafka/consumer/stop")).andExpect(status().isNoContent());
        verify(container, never()).stop(any(Runnable.class));
    }

    @Test
    void reportsRunningState() throws Exception {
        when(container.isRunning()).thenReturn(true);
        mvc.perform(get("/api/local/kafka/consumer"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.listenerId").value("vote-log-listener"))
                .andExpect(jsonPath("$.running").value(true));
    }

    @Test
    void missingContainerReturns404() throws Exception {
        when(registry.getListenerContainer("vote-log-listener")).thenReturn(null);
        mvc.perform(post("/api/local/kafka/consumer/stop")).andExpect(status().isNotFound());
        mvc.perform(post("/api/local/kafka/consumer/start")).andExpect(status().isNotFound());
        mvc.perform(get("/api/local/kafka/consumer")).andExpect(status().isNotFound());
    }

    @Test
    void controllerIsOnlyRegisteredInLocalProfile() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(KafkaListenerEndpointRegistry.class, () -> registry);
            context.register(LocalConsumerController.class);
            context.refresh();
            assertThat(context.getBeansOfType(LocalConsumerController.class)).isEmpty();
        }
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("local");
            context.registerBean(KafkaListenerEndpointRegistry.class, () -> registry);
            context.register(LocalConsumerController.class);
            context.refresh();
            assertThat(context.getBeansOfType(LocalConsumerController.class)).hasSize(1);
        }
    }
}
