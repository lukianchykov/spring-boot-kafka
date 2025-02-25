package com.learnkafka.libraryeventconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

//@Component
@Slf4j
public class LibraryEventConsumer {

    @Autowired
    private LibraryEventsService libraryEventsService;

    @KafkaListener(
        topics = {"library-events"}
        , autoStartup = "${libraryListener.startup:true}"
        , groupId = "library-events-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        log.info("ConsumerRecord : {} ", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}