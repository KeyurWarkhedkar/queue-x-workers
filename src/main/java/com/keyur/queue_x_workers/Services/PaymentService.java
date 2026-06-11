package com.keyur.queue_x_workers.Services;

public interface PaymentService {
    public boolean processPayment(Long orderId, Double amount, String idempotencyKey);
}
