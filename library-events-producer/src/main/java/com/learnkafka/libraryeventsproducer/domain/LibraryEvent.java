package com.learnkafka.libraryeventsproducer.domain;

public record LibraryEvent(
        Integer LibraryEventId,
        LibraryEventType libraryEventType,
        Book book
) {
}
