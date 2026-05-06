package com.learnkafka.mapper;

import com.learnkafka.dto.BookRequest;
import com.learnkafka.dto.LibraryEventRequest;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventMapper {

    public LibraryEvent toEntity(LibraryEventRequest libraryEventRequest) {
        if (libraryEventRequest == null) {
            return null;
        }

        Book book = null;
        if (libraryEventRequest.getBook() != null) {
            book = Book.builder()
                    .bookId(libraryEventRequest.getBook().getBookId())
                    .bookName(libraryEventRequest.getBook().getBookName())
                    .bookAuthor(libraryEventRequest.getBook().getBookAuthor())
                    .build();
        }

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(libraryEventRequest.getLibraryEventId())
                .libraryEventType(libraryEventRequest.getLibraryEventType())
                .book(book)
                .build();

        if (book != null) {
            book.setLibraryEvent(libraryEvent);
        }

        return libraryEvent;
    }

    // You can add toDto methods if needed for responses
}
