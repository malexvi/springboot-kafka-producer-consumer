package com.learnkafka.entity;

import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

//TODO - ADD BETTER SRP
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

    @Id
    Integer bookId;
    String bookName;
    String bookAuthor;

    @OneToOne
    @JoinColumn(name = "libraryEventId")
    @JsonBackReference
    private LibraryEvent libraryEvent;
}
