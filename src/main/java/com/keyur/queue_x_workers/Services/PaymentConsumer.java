package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Entities.OutboxEvent;
import com.keyur.queue_x_workers.Entities.Payment;
import com.keyur.queue_x_workers.Enums.EventType;
import com.keyur.queue_x_workers.Enums.OutboxStatus;
import com.keyur.queue_x_workers.Repositories.OutboxEventRepository;
import com.keyur.queue_x_workers.Repositories.PaymentRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
public class PaymentConsumer {
    // Fields
    private final MessageQueue messageQueue;
    private final IdempotencyService idempotencyService;
    private final PaymentService paymentService;
    private final OutboxEventRepository outboxEventRepository;
    private final PaymentRepository paymentRepository;

    @Scheduled(fixedDelay = 100)
    @Transactional
    public void consumeEvent() {
        EventDto eventDto = messageQueue.consume(QueueConstants.paymentQueue);
        if(eventDto == null) return;

        // Step 2: retry loop with exponential backoff
        boolean success = false;
        String failureReason = null;

        for(int i = 0; i < 3; i++) {
            try {
                success = paymentService.processPayment(
                        eventDto.getOrderId(),
                        eventDto.getAmount(),
                        eventDto.getEventId()  // gateway idempotency key
                );
                break;
            } catch(Exception e) {
                failureReason = e.getMessage();
                log.warn("Payment attempt {} failed for orderId={}, reason={}",
                        i + 1, eventDto.getOrderId(), e.getMessage());
                if(i < 2) {
                    try {
                        Thread.sleep((long) Math.pow(2, i) * 1000); // 1s then 2s
                    } catch(InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // Step 3: save result in short transaction
        savePaymentResult(eventDto, success, failureReason);
    }


    public void savePaymentResult(EventDto eventDto, boolean success, String failureReason) {
        // Step 1: idempotency check FIRST before any payment call
        if(!idempotencyService.saveRecord(eventDto.getEventId(), eventDto.getOrderId())) {
            log.info("Already processed eventId={}, skipping", eventDto.getEventId());
            return;
        }

        // Save payment record
        Payment payment = new Payment();
        payment.setOrderId(eventDto.getOrderId());
        payment.setAmount(eventDto.getAmount());
        payment.setStatus(success ? "SUCCESS" : "FAILED");
        payment.setFailureReason(failureReason);
        payment.setCreatedAt(LocalDateTime.now());
        paymentRepository.save(payment);

        if(success) {
            // order status update + notification
            writeToOutbox(eventDto, eventDto.getAmount(), EventType.PAYMENT_SUCCESS);
            writeToOutbox(eventDto, eventDto.getAmount(), EventType.PAYMENT_SUCCESS_NOTIFICATION);
            log.info("Payment successful for orderId={}",
                    eventDto.getOrderId());
        } else {
            // order status update + notification
            writeToOutbox(eventDto, 0, EventType.PAYMENT_FAILED);
            writeToOutbox(eventDto, 0, EventType.PAYMENT_FAILED_NOTIFICATION);
            log.warn("Payment failed for orderId={} after max retries", eventDto.getOrderId());
        }
    }

    private void writeToOutbox(EventDto event, double amount, EventType eventType) {
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setIdempotencyKey(UUID.randomUUID().toString());  // fresh id per row
        outboxEvent.setOrderId(event.getOrderId());
        outboxEvent.setAmount(amount);
        outboxEvent.setProductId(event.getProductId());
        outboxEvent.setQuantity(event.getQuantity());
        outboxEvent.setUserId(event.getUserId());
        outboxEvent.setEventType(eventType);
        outboxEvent.setStatus(OutboxStatus.PENDING);
        outboxEvent.setRetryCount(0);
        outboxEvent.setCreatedAt(LocalDateTime.now());
        outboxEventRepository.save(outboxEvent);
    }
}
