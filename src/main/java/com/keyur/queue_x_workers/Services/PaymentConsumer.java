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
import org.slf4j.MDC;
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

        // Fresh MDC for this one order's entire payment attempt (including all retries below) —
        // cleared in finally so it never leaks into the next unrelated order on this thread.
        try {
            MDC.put("orderId", String.valueOf(eventDto.getOrderId()));
            log.info("sagaStep=PAYMENT_CONSUMED eventId={} orderId={} amount={}",
                    eventDto.getEventId(), eventDto.getOrderId(), eventDto.getAmount());

            // Step 2: retry loop with exponential backoff
            boolean success = false;
            String failureReason = null;

            if(eventDto.getAmount() == 1000.00) {
                savePaymentResult(eventDto, success, "Payment Gateway Error. Please try again later!");
                MDC.clear();
                return;
            }

            for(int i = 0; i < 3; i++) {
                try {
                    log.info("sagaStep=PAYMENT_ATTEMPT attemptNumber={} orderId={}", i + 1, eventDto.getOrderId());
                    success = paymentService.processPayment(
                            eventDto.getOrderId(),
                            eventDto.getAmount(),
                            eventDto.getEventId()  // gateway idempotency key
                    );
                    break;
                } catch(Exception e) {
                    failureReason = e.getMessage();
                    log.warn("sagaStep=PAYMENT_ATTEMPT_FAILED attemptNumber={} orderId={} reason={}",
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
        } finally {
            MDC.clear();
        }
    }


    public void savePaymentResult(EventDto eventDto, boolean success, String failureReason) {
        // Step 1: idempotency check FIRST before any payment call
        if(!idempotencyService.saveRecord(eventDto.getEventId(), eventDto.getOrderId())) {
            log.info("sagaStep=PAYMENT_RESULT_SKIPPED reason=ALREADY_PROCESSED eventId={} orderId={}", eventDto.getEventId(), eventDto.getOrderId());
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
            log.info("sagaStep=PAYMENT_RESULT result=SUCCESS orderId={}", eventDto.getOrderId());
        } else {
            // order status update + notification
            writeToOutbox(eventDto, 0, EventType.PAYMENT_FAILED);
            writeToOutbox(eventDto, 0, EventType.PAYMENT_FAILED_NOTIFICATION);
            log.warn("sagaStep=PAYMENT_RESULT result=FAILED orderId={} reason={}", eventDto.getOrderId(), failureReason);
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

        log.info("sagaStep=OUTBOX_WRITE eventType={} orderId={} outboxStatus=PENDING", eventType, event.getOrderId());
    }
}