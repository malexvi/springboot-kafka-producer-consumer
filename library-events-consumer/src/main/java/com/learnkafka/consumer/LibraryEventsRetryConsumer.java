package com.learnkafka.consumer;

import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    @Autowired
    private LibraryEventsService libraryEventsService;

    @KafkaListener(
            topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:true}", // if this value is not set as part of the app.yml or TEST case then by default this is going to start the consumer
            groupId = "retry-listener-group"
    )
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws Exception {
        log.info("ConsumerRecord in Retry Consumer: {} ", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
