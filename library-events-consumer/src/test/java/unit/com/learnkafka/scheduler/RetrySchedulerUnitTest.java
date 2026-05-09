package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RetrySchedulerUnitTest {

    @Mock
    private FailureRecordRepository failureRecordRepository;

    @Mock
    private LibraryEventsService libraryEventsService;

    @InjectMocks
    private RetryScheduler retryScheduler;

    @Test
    void shouldProcessFailedRecordsAndChangeStatusToSuccess() throws JsonProcessingException {

        FailureRecord failureRecord = FailureRecord.builder()
                .id(1)
                .topic("library-events")
                .key_value(123)
                .errorRecord("{\"libraryEventId\":123}")
                .partition(0)
                .offset_value(1L)
                .status(LibraryEventsConsumerConfig.RETRY)
                .build();

        when(failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY))
                .thenReturn(List.of(failureRecord));


        retryScheduler.retryFailedRecords();


        verify(libraryEventsService, times(1)).processLibraryEvent(any(ConsumerRecord.class));

        assertEquals(LibraryEventsConsumerConfig.SUCCESS, failureRecord.getStatus());
    }

    @Test
    void shouldThrowRuntimeExceptionWhenProcessingFails() throws JsonProcessingException {

        FailureRecord failureRecord = FailureRecord.builder()
                .id(1)
                .topic("library-events")
                .key_value(123)
                .errorRecord("{\"libraryEventId\":123}")
                .partition(0)
                .offset_value(1L)
                .status(LibraryEventsConsumerConfig.RETRY)
                .build();

        when(failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY))
                .thenReturn(List.of(failureRecord));

        doThrow(new RuntimeException("Simulated Error"))
                .when(libraryEventsService).processLibraryEvent(any(ConsumerRecord.class));


        assertThrows(RuntimeException.class, () -> retryScheduler.retryFailedRecords());

        assertEquals(LibraryEventsConsumerConfig.RETRY, failureRecord.getStatus());
    }
}