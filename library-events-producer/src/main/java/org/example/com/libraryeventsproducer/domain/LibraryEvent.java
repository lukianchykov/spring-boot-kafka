package org.example.com.libraryeventsproducer.domain;

public record LibraryEvent(
    Integer libraryEventId,
    LibraryEventType libraryEventType,
    Book book
) {}
