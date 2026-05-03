package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.assertions.LibraryEventAssert;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// 1. @SpringBootTest: Sobe a aplicação Spring inteira (como se estivesse rodando em produção).
// RANDOM_PORT: Inicia o servidor Tomcat em uma porta aleatória para evitar conflitos de porta na sua máquina.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// 2. @EmbeddedKafka: Inicia uma instância do Kafka e do Zookeeper na memória apenas para esse teste!
// Ele também já cria o tópico "library-events" automaticamente antes do teste começar.
@EmbeddedKafka(topics = {"library-events"})
// 3. @TestPropertySource: Como subimos um Kafka na memória, ele vai rodar numa porta aleatória (não na 9092 do Docker).
// Essa anotação avisa o application.yml para ignorar o localhost:9092 e usar os brokers deste Kafka embutido.
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsConsumerIntegrationTest { //@SpringBootTest + @EmbeddedKafka (When integration test, don't mock)

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @MockitoSpyBean
    LibraryEventsService libraryEventsServiceSpy;


    @BeforeEach
    void setUp() {
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws Exception {

        // Arrange
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent event = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        // mantém consistência da relação bidirecional
        book.setLibraryEvent(event);

        String eventJson = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.send("library-events", eventJson);

        // Assert
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {

                    var events = libraryEventsRepository.findAll();

                    assertThat(events)
                            .hasSize(1)
                            .first()
                            .satisfies(savedEvent -> {
                                assertThat(savedEvent.getLibraryEventType())
                                        .isEqualTo(LibraryEventType.NEW);

                                assertThat(savedEvent.getBook().getBookId())
                                        .isEqualTo(456);

                                assertThat(savedEvent.getBook().getBookName())
                                        .isEqualTo("Kafka Using Spring Boot");
                            });
                });
    }

    @Test
    void publishNewLibraryEventAsyncDefault() throws InterruptedException, JsonProcessingException {
        String event = """
                {
                  "libraryEventId": null,
                  "libraryEventType": "NEW",
                  "book": {
                    "bookId": 456,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        kafkaTemplate.sendDefault(event);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any(ConsumerRecord.class));

        List<LibraryEvent> eventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertThat(eventList).hasSize(1)
                .extracting(LibraryEvent::getLibraryEventType)
                .containsExactly(LibraryEventType.NEW);

        assertThat(eventList)
                .extracting(LibraryEvent::getLibraryEventId)
                .isNot(null);

        eventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishNewLibraryEventAsyncDefaultWithAssertClass() throws Exception {

        // Arrange
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent event = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        // mantém consistência do relacionamento JPA
        book.setLibraryEvent(event);

        String eventJson = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.sendDefault(eventJson);

        // Aguarda processamento assíncrono
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {

                    verify(libraryEventsConsumerSpy, times(1))
                            .onMessage(isA(ConsumerRecord.class));

                    verify(libraryEventsServiceSpy, times(1))
                            .processLibraryEvent(any(ConsumerRecord.class));

                    List<LibraryEvent> eventList = libraryEventsRepository.findAll();

                    assertThat(eventList)
                            .hasSize(1)
                            .first()
                            .satisfies(savedEvent ->
                                    LibraryEventAssert.assertThatLibraryEvent(savedEvent)
                                            .hasType(LibraryEventType.NEW)
                                            .hasBookId(456)
                                            .hasNonNullId()
                            );
                });
    }


    //  kafkaTemplate.sendDefault(...)   ← (producer)
    //          ↓
    //  Kafka (broker — no teste é o EmbeddedKafka)
    //          ↓
    //  Spring Kafka Listener Container
    //        ↓
    //  @KafkaListener (onMessage)       ← (consumer)
    //          ↓
    //   lógica (service, repository…)
    // kafkaTemplate.send() → Spring chama @KafkaListener
    @Test
    void publishUpdateLibraryEventAsyncDefaultWithAssertClass() throws Exception {

        // =========================
        // GIVEN - estado inicial
        // =========================
        Integer id = eventSavedOnDatabase().getLibraryEventId();

        // =========================
        // WHEN - evento UPDATE
        // =========================
        kafkaTemplate.sendDefault(buildUpdateEventPayload(id));

        // =========================
        // THEN - valida resultado
        // =========================
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {

                    // só 1 evento foi enviado
                    verify(libraryEventsConsumerSpy, times(1))
                            .onMessage(isA(ConsumerRecord.class));

                    verify(libraryEventsServiceSpy, times(1))
                            .processLibraryEvent(isA(ConsumerRecord.class));

                    List<LibraryEvent> events = libraryEventsRepository.findAll();

                    assertThat(events)
                            .hasSize(1) // não criou outro registro
                            .first()
                            .satisfies(savedEvent ->
                                    LibraryEventAssert.assertThatLibraryEvent(savedEvent)
                                            .hasType(LibraryEventType.UPDATE)
                                            .hasBookId(456)
                                            .hasEqualName("New book name")
                                            .hasEqualAuthorName("New book Author")
                                            .hasNonNullId()
                            );
                });
    }

    private LibraryEvent eventSavedOnDatabase() {
        Book book = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent existing = LibraryEvent.builder()
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        // manter consistência JPA
        book.setLibraryEvent(existing);

        // salva e deixa o banco gerar o ID
        return libraryEventsRepository.save(existing);

    }

    private String buildUpdateEventPayload(Integer id) throws JsonProcessingException {
        Book updatedBook = Book.builder()
                .bookId(456) // mesmo bookId (você controla)
                .bookName("New book name")
                .bookAuthor("New book Author")
                .build();

        LibraryEvent updateEvent = LibraryEvent.builder()
                .libraryEventId(id) // ESSENCIAL: usar o ID do banco
                .libraryEventType(LibraryEventType.UPDATE)
                .book(updatedBook)
                .build();

        updatedBook.setLibraryEvent(updateEvent);

        return objectMapper.writeValueAsString(updateEvent);
    }

}