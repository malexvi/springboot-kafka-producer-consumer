package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent: {} ", libraryEvent);

        switch(libraryEvent.getLibraryEventType()){
            case NEW:
                // NEW = SAVE
                save(libraryEvent);
                break;
            case UPDATE:
                //UPDATE = ...
                break;
            default:
                log.info("Invalid library Event Type");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);// Why??
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfuly Persisted the library Event: {} ", libraryEvent);
    }
}
