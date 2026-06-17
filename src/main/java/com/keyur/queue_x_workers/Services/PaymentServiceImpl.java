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
        // No MDC.put/clear here on purpose — this method is always called from
        // PaymentConsumer's retry loop, on the same thread, which already set
        // MDC orderId for the whole attempt. These log lines inherit it for free.
        log.info("sagaStep=PAYMENT_GATEWAY_CALL orderId={} amount={} idempotencyKey={}",
                orderId, amount, idempotencyKey);

        // Simulate network delay
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 30% chance of failure
        if(random.nextInt(100) < 30) {
            log.warn("sagaStep=PAYMENT_GATEWAY_RESPONSE result=ERROR orderId={}", orderId);
            throw new RuntimeException("Payment gateway error. Please try again.");
        }

        log.info("sagaStep=PAYMENT_GATEWAY_RESPONSE result=SUCCESS orderId={}", orderId);
        return true;
    }
}