package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.Repositories.ProductRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@AllArgsConstructor
public class PaymentFailedConsumer {
    // Fields
    private final MessageQueue messageQueue;
    private final IdempotencyService idempotencyService;
    private final ProductRepository productRepository;
    private final OutboxEventRepository outboxEventRepository;

    @Scheduled(fixedDelay = 130)
    @Transactional
    public void consumeEvent() {
        EventDto event = messageQueue.consume(QueueConstants.paymentFailedQueue);
        if(event == null) return;
        processPaymentFailure(event);
    }

    public void processPaymentFailure(EventDto event) {
        // Fresh MDC per event on this scheduler thread — cleared in finally so it
        // never leaks into the next unrelated order processed on the same thread.
        try {
            MDC.put("orderId", String.valueOf(event.getOrderId()));
            log.info("sagaStep=PAYMENT_FAILED_CONSUMED eventId={} orderId={}", event.getEventId(), event.getOrderId());

            // Blind insert to maintain idempotency of the inventory undo operation
            if(!idempotencyService.saveRecord(event.getEventId(), event.getOrderId())) {
                log.info("sagaStep=COMPENSATION_SKIPPED reason=ALREADY_PROCESSED eventId={} orderId={}", event.getEventId(), event.getOrderId());
                return;
            }

            // Increment the stock
            productRepository.atomicIncrement(event.getProductId(), event.getQuantity());
            log.info("sagaStep=COMPENSATING_TRANSACTION action=STOCK_RESTORED orderId={} productId={} quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());

            // Write to outbox to for the Order Api to consume and mark order as Failed
            writeToOutbox(event, 0);
        } finally {
            MDC.clear();
        }
    }

    private void writeToOutbox(EventDto event, double amount) {
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setIdempotencyKey(UUID.randomUUID().toString());  // fresh id per row
        outboxEvent.setOrderId(event.getOrderId());
        outboxEvent.setAmount(amount);
        outboxEvent.setProductId(event.getProductId());
        outboxEvent.setQuantity(event.getQuantity());
        outboxEvent.setUserId(event.getUserId());
        outboxEvent.setEventType(EventType.PAYMENT_FAILED_FOR_ORDER_API);
        outboxEvent.setStatus(OutboxStatus.PENDING);
        outboxEvent.setRetryCount(0);
        outboxEvent.setCreatedAt(LocalDateTime.now());
        outboxEventRepository.save(outboxEvent);

        log.info("sagaStep=OUTBOX_WRITE eventType=PAYMENT_FAILED_FOR_ORDER_API orderId={} outboxStatus=PENDING", event.getOrderId());
    }
}