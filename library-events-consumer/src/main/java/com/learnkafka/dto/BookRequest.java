package com.learnkafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BookRequest {
    @NotNull(message = "BookId cannot be null")
    private Integer bookId;
    @NotBlank(message = "BookName cannot be blank")
    private String bookName;
    @NotBlank(message = "BookAuthor cannot be blank")
    private String bookAuthor;
}
