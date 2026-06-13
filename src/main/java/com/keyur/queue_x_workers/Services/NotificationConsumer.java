package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.NotificationIdempotencyRecord;
import com.keyur.queue_x_workers.Enums.IdempotencyStatus;
import com.keyur.queue_x_workers.Repositories.NotificationIdempotencyRecordRepository;
import lombok.AllArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


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
        // Blind insert to maintain idempotency
        idempotencyService.saveNotificationIdempotencyRecord(eventDto.getEventId(), eventDto.getOrderId(), IdempotencyStatus.INIT);

        int affected = notificationIdempotencyRecordRepository.atomicStatusChange(IdempotencyStatus.PROCESSING, IdempotencyStatus.INIT, eventDto.getEventId());

        if(affected == 0) {
            return;
        }

        emailService.sendEmail(eventDto);

        notificationIdempotencyRecordRepository.atomicStatusChange(IdempotencyStatus.SUCCESS, IdempotencyStatus.PROCESSING, eventDto.getEventId());
    }
}
