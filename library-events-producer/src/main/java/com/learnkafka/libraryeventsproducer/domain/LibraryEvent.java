package com.learnkafka.libraryeventsproducer.domain;

public record LibraryEvent(
        LibraryEventType libraryEventType,
        Integer LibraryEventId,
        Book book
) {
}
