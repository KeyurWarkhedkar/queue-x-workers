package com.keyur.queue_x_workers.Repositories;

import com.keyur.queue_x_workers.Entities.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
}
