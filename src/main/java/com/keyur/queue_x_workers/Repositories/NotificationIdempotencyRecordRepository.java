package com.keyur.queue_x_workers.Repositories;

import com.keyur.queue_x_workers.Entities.NotificationIdempotencyRecord;
import com.keyur.queue_x_workers.Enums.IdempotencyStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface NotificationIdempotencyRecordRepository extends JpaRepository<NotificationIdempotencyRecord, Long> {
    Optional<NotificationIdempotencyRecord> findByMessageId(String messageId);

    @Modifying
    @Query("UPDATE NotificationIdempotencyRecord nir " +
            "SET nir.idempotencyStatus = :newIdempotencyStatus " +
    "WHERE nir.messageId = :messageId AND nir.idempotencyStatus = :oldIdempotencyStatus")
    int atomicStatusChange(@Param("newIdempotencyStatus")IdempotencyStatus newIdempotencyStatus, @Param("oldIdempotencyStatus")IdempotencyStatus oldIdempotencyStatus, @Param("messageId") String messageId);


}
