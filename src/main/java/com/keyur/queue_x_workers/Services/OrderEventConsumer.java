package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Entities.Product;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.IdempotencyRecordRepository;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.Repositories.ProductRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@AllArgsConstructor
public class OrderEventConsumer {
    // Fields
    private final MessageQueue messageQueue;
    private final ProductRepository productRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final IdempotencyRecordRepository idempotencyRecordRepository;
    private final IdempotencyService idempotencyService;


    @Scheduled(fixedDelay = 100)
    @Transactional
    public void inventoryWorker() {
        EventDto event = messageQueue.consume(QueueConstants.orderQueue);
        if(event == null) return;
        processEvent(event);
    }


    public void processEvent(EventDto event) {
        // Step 1: blind INSERT as atomic lock
        if(!idempotencyService.saveRecord(event.getEventId(), event.getOrderId())) {
            return;
        }

        // Step 2: atomic decrement
        int affected = productRepository.atomicDecrement(
                event.getProductId(), event.getQuantity()
        );

        if(affected == 0) {
            // out of stock
            writeToOutbox(event, 0, EventType.OUT_OF_STOCK);
            writeToOutbox(event, 0, EventType.OUT_OF_STOCK_NOTIFICATION);
            return;
        }

        // Step 3: calculate amount on backend
        Product product = productRepository.findById(event.getProductId())
                .orElseThrow(() -> new RuntimeException("Product not found"));
        double amount = product.getPrice() * event.getQuantity();

        // Step 4: write success outbox
        writeToOutbox(event, amount, EventType.INVENTORY_SUCCESS);
    }

    private void writeToOutbox(EventDto event, double amount, EventType eventType) {
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setIdempotencyKey(UUID.randomUUID().toString());  // fresh id per row
        outboxEvent.setOrderId(event.getOrderId());
        outboxEvent.setAmount(amount);
        outboxEvent.setProductId(event.getProductId());
        outboxEvent.setQuantity(event.getQuantity());
        outboxEvent.setEventType(eventType);
        outboxEvent.setStatus(OutboxStatus.PENDING);
        outboxEvent.setRetryCount(0);
        outboxEvent.setUserId(event.getUserId());
        outboxEvent.setCreatedAt(LocalDateTime.now());
        outboxEventRepository.save(outboxEvent);
    }
}
