package com.learnkafka.libraryeventconsumer.service;

import com.learnkafka.libraryeventconsumer.entity.FailureRecord;
import com.learnkafka.libraryeventconsumer.jpa.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.stereotype.Service;

@Service
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus){
        var failureRecord = new FailureRecord(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                recordStatus);

        failureRecordRepository.save(failureRecord);

    }
}