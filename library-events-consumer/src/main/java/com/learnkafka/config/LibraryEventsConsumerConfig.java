package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
// @EnableKafka Needed for older versions of Spring Boot
public class LibraryEventsConsumerConfig {

    public static final String SUCCESS = "SUCCESS";
    private final KafkaProperties properties;
    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;

    @Value("${topics.retry}")
    private String retry;

    @Value("${topics.dlt}")
    private String dlt;

    public LibraryEventsConsumerConfig(KafkaProperties kafkaProperties) {
        this.properties = kafkaProperties;
    }

    public DefaultErrorHandler defaultErrorHandler() {

        var exceptionsToIgnore = List.of(
                IllegalArgumentException.class,
                NullPointerException.class
        );


        var finexBackOff = new FixedBackOff(1000L, 2); // Retry the failed record twice with the delay of 1s in between

        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L); // dalay of 1s
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setInitialInterval(2_000L);


        var errorHandler = new DefaultErrorHandler(
                //publishingRecoverer(),
                consumerRecordRecoverer,
                finexBackOff
        );

//        exceptionsToIgnore.forEach(
//                errorHandler::addNotRetryableExceptions
//                //errorHandler::addRetryableExceptions We need to create the list of what
//        );

        errorHandler
                .setRetryListeners((// When necessary, we can see the logs, this way we'll know what's happening
                        record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in retry Listener, Exception: {}, deliveryAttempt: {} "
                            , ex.getMessage(), deliveryAttempt);
                });
        return errorHandler;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in publishingRecoverer: {}", e.getMessage(), e);

        var record = (ConsumerRecord<Integer, String>) consumerRecord;

        if (e.getCause() instanceof RecoverableDataAccessException) {
            // recovery logic
            log.info("Inside Recovery");
            failureService.saveFailureRecord(record, e, RETRY);
        } else {
            // non recovery logic
            log.info("Inside Non Recovery");
            failureService.saveFailureRecord(record, e, DEAD);

        }
    };

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retry, r.partition());
                    } else {
                        return new TopicPartition(dlt, r.partition());
                    }
                });
        return recoverer;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        factory.setCommonErrorHandler(defaultErrorHandler());
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    //@Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactoryConcurrent(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        factory.setConcurrency(3);
        // Now, we spun up three consumer on three threads?
        // This way our consumer will pull from three diferent partitions, is that right?
        // 2026-05-01T09:31:15.321-04:00  INFO 102808 --- [ntainer#0-0-C-1] k.c.c.i.ConsumerRebalanceListenerInvoker : [Consumer clientId=consumer-library-events-listener-group-1, groupId=library-events-listener-group] Adding newly assigned partitions: library-events-0
        //2026-05-01T09:31:15.321-04:00  INFO 102808 --- [ntainer#0-1-C-1] k.c.c.i.ConsumerRebalanceListenerInvoker : [Consumer clientId=consumer-library-events-listener-group-2, groupId=library-events-listener-group] Adding newly assigned partitions: library-events-1
        //2026-05-01T09:31:15.321-04:00  INFO 102808 --- [ntainer#0-2-C-1] k.c.c.i.ConsumerRebalanceListenerInvoker : [Consumer clientId=consumer-library-events-listener-group-3, groupId=library-events-listener-group] Adding newly assigned partitions: library-events-2
        //2026-05-01T09:31:15.340-04:00  INFO 102808 --- [ntainer#0-1-C-1] o.a.k.c.c.internals.ConsumerUtils        : Setting offset for partition library-events-1 to the committed offset FetchPosition{offset=2, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[127.0.0.1:9093 (id: 2 rack: null)], epoch=5}}
        //2026-05-01T09:31:15.340-04:00  INFO 102808 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerUtils        : Setting offset for partition library-events-0 to the committed offset FetchPosition{offset=1, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[127.0.0.1:9092 (id: 1 rack: null)], epoch=4}}
        //2026-05-01T09:31:15.340-04:00  INFO 102808 --- [ntainer#0-2-C-1] o.a.k.c.c.internals.ConsumerUtils        : Setting offset for partition library-events-2 to the committed offset FetchPosition{offset=2, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[127.0.0.1:9094 (id: 3 rack: null)], epoch=5}}
        //2026-05-01T09:31:15.342-04:00  INFO 102808 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-0]
        //2026-05-01T09:31:15.342-04:00  INFO 102808 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-1]
        //2026-05-01T09:31:15.342-04:00  INFO 102808 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : library-events-listener-group: partitions assigned: [library-events-2]
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }
}
