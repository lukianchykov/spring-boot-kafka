package org.example.com.libraryeventsproducer.controller;

import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.example.com.libraryeventsproducer.domain.LibraryEvent;
import org.example.com.libraryeventsproducer.domain.LibraryEventType;
import org.example.com.libraryeventsproducer.producer.LibraryEventsProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
        @RequestBody LibraryEvent libraryEvent
    ) throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("libraryEvent: {}", libraryEvent);
        libraryEventsProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventsProducer.sendLibraryEventApproachThree(libraryEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType()))  {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }
}
