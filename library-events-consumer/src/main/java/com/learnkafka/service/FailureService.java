package com.learnkafka.service;

import com.learnkafka.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailureRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {

        String errorMessage = (e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();

        var failureRecord = com.learnkafka.entity.FailureRecord.builder()
                .topic(consumerRecord.topic())
                .key_value(consumerRecord.key())
                .errorRecord(consumerRecord.value())
                .partition(consumerRecord.partition())
                .offset_value(consumerRecord.offset())
                .exception(errorMessage)
                .status(status)
                .build();

        failureRecordRepository.save(failureRecord);
    }
}