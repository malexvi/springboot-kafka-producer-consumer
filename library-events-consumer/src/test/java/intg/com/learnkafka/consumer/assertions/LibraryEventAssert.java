package com.learnkafka.consumer.assertions;

import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class LibraryEventAssert extends AbstractAssert<LibraryEventAssert, LibraryEvent> {

    public LibraryEventAssert(LibraryEvent actual) {
        super(actual, LibraryEventAssert.class);
    }

    public static LibraryEventAssert assertThatLibraryEvent(LibraryEvent actual) {
        return new LibraryEventAssert(actual);
    }

    public LibraryEventAssert isValidLibraryEvent() {

        isNotNull();

        assertThat(actual.getLibraryEventType())
                .as("libraryEventType")
                .isNotNull();

        assertThat(actual.getLibraryEventId())
                .as("libraryEventId")
                .isNotNull();

        assertThat(actual.getBook())
                .as("book")
                .isNotNull();

        assertThat(actual.getBook().getBookId())
                .as("bookId")
                .isEqualTo(456);

        return this;
    }

    public LibraryEventAssert hasType(LibraryEventType expectedType) {

        isNotNull();

        assertThat(actual.getLibraryEventType())
                .as("libraryEventType")
                .isEqualTo(expectedType);

        return this;
    }

    public LibraryEventAssert hasBookId(int expectedId) {

        isNotNull();

        assertThat(actual.getBook())
                .as("book")
                .isNotNull();

        assertThat(actual.getBook().getBookId())
                .as("bookId")
                .isEqualTo(expectedId);

        return this;
    }

    public LibraryEventAssert hasNonNullId() {

        isNotNull();

        assertThat(actual.getLibraryEventId())
                .as("libraryEventId")
                .isNotNull();

        return this;
    }
}