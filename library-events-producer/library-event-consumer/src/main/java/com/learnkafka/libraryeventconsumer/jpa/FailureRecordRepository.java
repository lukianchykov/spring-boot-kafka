package com.learnkafka.libraryeventconsumer.jpa;

import java.util.List;

import com.learnkafka.libraryeventconsumer.entity.FailureRecord;

import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {

    List<FailureRecord> findAllByStatus(String status);
}