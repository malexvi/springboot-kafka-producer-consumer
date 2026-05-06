package com.learnkafka.dto;

import com.learnkafka.entity.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LibraryEventRequest {
    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NotNull(message = "Book cannot be null")
    @Valid
    private BookRequest book;
}
