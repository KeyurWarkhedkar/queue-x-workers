package com.keyur.queue_x_workers.Repositories;

import com.keyur.queue_x_workers.Entities.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    @Query(value = """
            SELECT * FROM outbox_event 
            WHERE status = 'PENDING'
            ORDER BY created_at ASC 
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEvents();
}
