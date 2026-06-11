package com.keyur.queue_x_workers.Entities;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "payments")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private Double amount;

    private String status;        // SUCCESS | FAILED

    private String gatewayRef;    // returned by payment gateway

    private String failureReason; // populated if failed

    private LocalDateTime createdAt;
}
