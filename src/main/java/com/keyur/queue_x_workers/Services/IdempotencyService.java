package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.Entities.IdempotencyRecord;
import com.keyur.queue_x_workers.Entities.NotificationIdempotencyRecord;
import com.keyur.queue_x_workers.Enums.IdempotencyStatus;
import com.keyur.queue_x_workers.Repositories.IdempotencyRecordRepository;
import com.keyur.queue_x_workers.Repositories.NotificationIdempotencyRecordRepository;
import lombok.AllArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@AllArgsConstructor
public class IdempotencyService {

    private final IdempotencyRecordRepository idempotencyRecordRepository;
    private final NotificationIdempotencyRecordRepository notificationIdempotencyRecordRepository;

    @Transactional
    public boolean saveRecord(String messageId, Long orderId) {
        try {
            IdempotencyRecord record = new IdempotencyRecord();
            record.setMessageId(messageId);
            record.setOrderId(orderId);
            record.setCreatedAt(LocalDateTime.now());
            idempotencyRecordRepository.save(record);
        } catch (DataIntegrityViolationException e) {
            return false;
        }
        return true;
    }

    @Transactional
    public boolean saveNotificationIdempotencyRecord(String messageId, Long orderId, IdempotencyStatus idempotencyStatus) {
        try {
            NotificationIdempotencyRecord record = new NotificationIdempotencyRecord();
            record.setMessageId(messageId);
            record.setOrderId(orderId);
            record.setCreatedAt(LocalDateTime.now());
            record.setIdempotencyStatus(idempotencyStatus);
            notificationIdempotencyRecordRepository.save(record);
        } catch (DataIntegrityViolationException e) {
            return false;
        }
        return true;
    }
}
