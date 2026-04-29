package com.learnkafka.libraryeventsproducer.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

//TODO - ADD BETTER SRP
public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,

        @NotNull
        @Valid
        Book book
) {
}
