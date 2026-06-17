package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.NotificationIdempotencyRecord;
import com.keyur.queue_x_workers.Enums.IdempotencyStatus;
import com.keyur.queue_x_workers.Repositories.NotificationIdempotencyRecordRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@AllArgsConstructor
public class NotificationConsumer {
    // Fields
    private final MessageQueue messageQueue;
    private final EmailService emailService;
    private final IdempotencyService idempotencyService;
    private final NotificationIdempotencyRecordRepository notificationIdempotencyRecordRepository;

    @Scheduled(fixedDelay = 150)
    public void consumeNotificationEvent() {
        EventDto eventDto = messageQueue.consume(QueueConstants.notification);
        if(eventDto == null) return;
        processNotification(eventDto);
    }

    public void processNotification(EventDto eventDto) {
        // Fresh MDC per event on this scheduler thread — cleared in finally so it
        // never leaks into the next unrelated order processed on the same thread.
        try {
            MDC.put("orderId", String.valueOf(eventDto.getOrderId()));
            log.info("sagaStep=NOTIFICATION_CONSUMED eventId={} orderId={}", eventDto.getEventId(), eventDto.getOrderId());

            // Blind insert to maintain idempotency
            idempotencyService.saveNotificationIdempotencyRecord(eventDto.getEventId(), eventDto.getOrderId(), IdempotencyStatus.INIT);

            int affected = notificationIdempotencyRecordRepository.atomicStatusChange(IdempotencyStatus.PROCESSING, IdempotencyStatus.INIT, eventDto.getEventId());

            if(affected == 0) {
                log.info("sagaStep=NOTIFICATION_SKIPPED reason=ALREADY_PROCESSING_OR_DONE eventId={} orderId={}", eventDto.getEventId(), eventDto.getOrderId());
                return;
            }

            emailService.sendEmail(eventDto);

            notificationIdempotencyRecordRepository.atomicStatusChange(IdempotencyStatus.SUCCESS, IdempotencyStatus.PROCESSING, eventDto.getEventId());

            log.info("sagaStep=NOTIFICATION_SENT orderId={}", eventDto.getOrderId());
        } finally {
            MDC.clear();
        }
    }
}