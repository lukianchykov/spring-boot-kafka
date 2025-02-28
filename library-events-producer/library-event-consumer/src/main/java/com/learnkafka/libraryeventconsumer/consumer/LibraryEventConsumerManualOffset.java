package com.learnkafka.libraryeventconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
public class LibraryEventConsumerManualOffset implements AcknowledgingMessageListener<Integer,String> {

    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord in Manual Offset Consumer: {} ", consumerRecord );
        acknowledgment.acknowledge();
    }
}
