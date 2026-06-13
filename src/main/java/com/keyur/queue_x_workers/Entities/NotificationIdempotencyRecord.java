package com.keyur.queue_x_workers.Entities;

import com.keyur.queue_x_workers.Enums.IdempotencyStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "notification_idempotency_records",
        uniqueConstraints = {
                @UniqueConstraint(columnNames = "messageId")
        })
@Getter
@Setter
public class NotificationIdempotencyRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    private String messageId;

    private Long orderId;

    private IdempotencyStatus idempotencyStatus;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}


