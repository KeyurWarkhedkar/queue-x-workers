package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.Repositories.ProductRepository;
import lombok.AllArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

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
        // Blind insert to maintain idempotency of the inventory undo operation
        if(!idempotencyService.saveRecord(event.getEventId(), event.getOrderId())) {
            return;
        }

        // Increment the stock
        productRepository.atomicIncrement(event.getProductId(), event.getQuantity());

        // Write to outbox to for the Order Api to consume and mark order as Failed
        writeToOutbox(event, 0);
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
    }
}
