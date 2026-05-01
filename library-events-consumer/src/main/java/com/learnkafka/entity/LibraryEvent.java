package com.learnkafka.entity;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

    @Id
    @GeneratedValue
    Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEvent",
            cascade =CascadeType.ALL )
    @ToString.Exclude // out of memory error
    @JsonManagedReference
    private Book book;
}