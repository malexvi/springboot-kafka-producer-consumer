package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsControllerIntegrationTest {

    // Cliente HTTP fornecido pelo Spring para fazermos requisições (como se fosse um Postman)
    @Autowired
    TestRestTemplate restTemplate;

    // Referência ao nosso Kafka embutido que está rodando na memória
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    // Este é o nosso "Consumidor Espião". Ele será usado apenas para ir no Kafka e ver
    // se o Producer da nossa aplicação realmente enviou a mensagem correta.
    private Consumer<Integer, String> consumer;

    // O @BeforeEach roda ANTES de cada método @Test. Ele prepara o terreno.
    @BeforeEach
    void setUp() {
        // Pega as configurações padrões para conectar no Kafka embutido
        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));

        // Diz ao consumidor para ignorar mensagens velhas e ler apenas as "latest" (últimas) mensagens que chegarem a partir de agora
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Cria o Consumidor (Consumer) passando que a Chave é Integer e o Valor é String (JSON)
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();

        // Inscreve este consumidor "espião" para escutar o tópico "library-events" no Kafka embutido
        embeddedKafkaBroker.consumeFromEmbeddedTopics(consumer, "library-events");
    }

    // O @AfterEach roda DEPOIS de cada método @Test. Ele limpa a sujeira.
    @AfterEach
    void tearDown() {
        // Fecha o consumidor para não vazar recursos de memória
        consumer.close();
    }

    @Test
    void postLibraryEvent() {
        // ==========================================
        // GIVEN: PREPARAÇÃO DA REQUISIÇÃO HTTP
        // ==========================================
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString()); // Informa que vamos mandar um JSON

        // Pega o objeto de teste simulado (do seu TestUtil) e empacota junto com os headers
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);

        // ==========================================
        // WHEN: EXECUÇÃO (A MÁGICA ACONTECE AQUI)
        // ==========================================
        // Faz a requisição POST real para o Controller (http://localhost:porta-aleatoria/v1/libraryevent)
        // O Controller vai receber, chamar o Producer, e o Producer vai jogar no Kafka.
        var responseEntity = restTemplate
                .exchange("/v1/libraryevent", HttpMethod.POST,
                        httpEntity, LibraryEvent.class);

        // ==========================================
        // THEN: VERIFICAÇÕES (ASSERTIONS)
        // ==========================================

        // 1ª Verificação: Confirma se o Controller HTTP nos respondeu com "201 Created"
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        // Agora vem a parte do Kafka!
        // KafkaTestUtils.getRecords vai até o tópico e "espera" a mensagem chegar lá.
        // Isso é crucial porque o envio é assíncrono.
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        // 2ª Verificação: Garante que apenas 1 mensagem foi gravada no tópico do Kafka
        assert consumerRecords.count() == 1;

        // Lê a mensagem que foi encontrada no Kafka
        consumerRecords.forEach(record -> {

            // Pega o JSON (String) que está dentro do Kafka e converte de volta para o objeto Java (LibraryEvent)
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            System.out.println("libraryEventActual: " + libraryEventActual);

            // 3ª Verificação: O objeto que tiramos de DENTRO do Kafka é exatamente igual ao objeto que enviamos no HTTP?
            assertEquals(libraryEventActual, TestUtil.libraryEventRecord());
        });
    }
}