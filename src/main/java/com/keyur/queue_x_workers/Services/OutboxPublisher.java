package com.keyur.queue_x_workers.Services;


import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.DTOs.EventDto;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@AllArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final MessageQueue messageQueue;

    private String getQueueForEvent(EventType eventType) {
        return switch(eventType) {
            case OUT_OF_STOCK        -> QueueConstants.outOfStock;
            case OUT_OF_STOCK_NOTIFICATION -> QueueConstants.notification;
            case INVENTORY_SUCCESS   -> QueueConstants.paymentQueue;
            case PAYMENT_SUCCESS -> QueueConstants.paymentSuccessQueue;
            case PAYMENT_SUCCESS_NOTIFICATION -> QueueConstants.notification;
            case PAYMENT_FAILED -> QueueConstants.paymentFailedQueue;
            case PAYMENT_FAILED_NOTIFICATION -> QueueConstants.notification;
            case PAYMENT_FAILED_FOR_ORDER_API -> QueueConstants.paymentFailedForOrderApiQueue;
            default -> throw new IllegalArgumentException("Unknown event: " + eventType);
        };
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishQueueEvents() {

        List<OutboxEvent> events =
                outboxEventRepository.findPendingEvents();

        for (OutboxEvent event : events) {
            // Fresh MDC per event on this scheduler thread — OutboxPublisher runs independently
            // of whichever thread wrote the row, so orderId is re-supplied from the row itself,
            // every iteration, then cleared before the next event in this batch.
            try {
                MDC.put("orderId", String.valueOf(event.getOrderId()));

                // 1. Publish to Redis queue
                messageQueue.publish(
                        getQueueForEvent(event.getEventType()),
                        buildEventDto(event)
                );

                // 2. Mark as published
                event.setStatus(OutboxStatus.PUBLISHED);
                outboxEventRepository.save(event);

                log.info("sagaStep=OUTBOX_PUBLISH eventType={} orderId={}", event.getEventType(), event.getOrderId());

            } catch (Exception e) {

                log.error(
                        "sagaStep=OUTBOX_PUBLISH_FAILED eventType={} outboxEventId={} orderId={} retryCount={}",
                        event.getEventType(),
                        event.getId(),
                        event.getOrderId(),
                        event.getRetryCount(),
                        e
                );

                event.setRetryCount(event.getRetryCount() + 1);

                if (event.getRetryCount() >= 5) {
                    event.setStatus(OutboxStatus.FAILED);
                    log.error("sagaStep=OUTBOX_PUBLISH_EXHAUSTED eventType={} outboxEventId={} orderId={}",
                            event.getEventType(), event.getId(), event.getOrderId());
                }

                outboxEventRepository.save(event);
            } finally {
                MDC.clear();
            }
        }
    }

    private EventDto buildEventDto(OutboxEvent event) {
        EventDto dto = new EventDto();
        dto.setEventId(event.getIdempotencyKey());
        dto.setOrderId(event.getOrderId());
        dto.setEventType(event.getEventType());
        dto.setAmount(event.getAmount());
        dto.setProductId(event.getProductId());
        dto.setQuantity(event.getQuantity());
        dto.setUserId(event.getUserId());
        dto.setCreatedAt(event.getCreatedAt().toString());
        return dto;
    }
}