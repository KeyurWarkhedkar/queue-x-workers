package com.keyur.queue_x_workers.Repositories;

import com.keyur.queue_x_workers.Entities.IdempotencyRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface IdempotencyRecordRepository extends JpaRepository<IdempotencyRecord, Long> {
    Optional<IdempotencyRecord> findByMessageId(String messageId);
}
