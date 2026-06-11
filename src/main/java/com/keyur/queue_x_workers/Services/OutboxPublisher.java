package com.keyur.queue_x_workers.Services;


import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.DTOs.EventDto;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@AllArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final RedisMessageQueue redisMessageQueue;

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
            try {

                // 1. Publish to Redis queue
                redisMessageQueue.publish(
                        getQueueForEvent(event.getEventType()),
                        buildEventDto(event)
                );

                // 2. Mark as published
                event.setStatus(OutboxStatus.PUBLISHED);
                outboxEventRepository.save(event);

                log.info("Published " + event.getEventType() + " event orderId={}", event.getOrderId());

            } catch (Exception e) {

                log.error(
                        "Failed publishing " + event.getEventType() + " event id={}, retry={}",
                        event.getId(),
                        event.getRetryCount(),
                        e
                );

                event.setRetryCount(event.getRetryCount() + 1);

                if (event.getRetryCount() >= 5) {
                    event.setStatus(OutboxStatus.FAILED);
                    log.error(event.getEventType() + " event marked FAILED id={}", event.getId());
                }

                outboxEventRepository.save(event);
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
