package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
// @EnableKafka Needed for older versions of Spring Boot
public class LibraryEventsConsumerConfig {

    private final KafkaProperties properties;

    public LibraryEventsConsumerConfig(KafkaProperties kafkaProperties){
        this.properties = kafkaProperties;
    }
    public DefaultErrorHandler defaultErrorHandler(){

        var finexBackOff = new FixedBackOff(1000L, 2); // Retry the failed record twice with the delay of 1s in between

        var errorHandler = new DefaultErrorHandler(finexBackOff);

        errorHandler
                .setRetryListeners((
                        record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in retry Listener, Exception: {}, deliveryAttempt: {} "
                            , ex.getMessage(), deliveryAttempt);
                });
        return errorHandler;
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
