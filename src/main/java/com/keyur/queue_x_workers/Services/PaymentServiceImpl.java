package com.keyur.queue_x_workers.Services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Random;

@Slf4j
@Service
public class PaymentServiceImpl implements PaymentService {

    private final Random random = new Random();

    @Override
    public boolean processPayment(Long orderId, Double amount, String idempotencyKey) {
        log.info("Processing payment for orderId={}, amount={}, idempotencyKey={}",
                orderId, amount, idempotencyKey);

        // Simulate network delay
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 30% chance of failure
        if(random.nextInt(100) < 30) {
            log.warn("Payment failed for orderId={}", orderId);
            throw new RuntimeException("Payment gateway error. Please try again.");
        }

        log.info("Payment successful for orderId={}", orderId);
        return true;
    }
}
