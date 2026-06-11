package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
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

    @Scheduled(fixedDelay = 150)
    public void consumeNotificationEvent() {
        EventDto eventDto = messageQueue.consume(QueueConstants.notification);
        if(eventDto == null) return;
        processNotification(eventDto);
    }

    public void processNotification(EventDto eventDto) {
        // Blind insert to maintain idempotency
        if(!idempotencyService.saveRecord(eventDto.getEventId(), eventDto.getOrderId())) {
            return;
        }

        emailService.sendEmail(eventDto);
    }
}
