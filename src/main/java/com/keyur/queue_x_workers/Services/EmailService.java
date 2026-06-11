package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;
import com.keyur.queue_x_workers.Enums.EventType;
import org.springframework.stereotype.Service;

@Service
public class EmailService {
    public void sendEmail(EventDto eventDto) {
        System.out.println("To: " + eventDto.getUserId());
        System.out.println(buildEmail(eventDto.getEventType()));
    }

    private String buildEmail(EventType eventType) {
        return switch (eventType) {
            case OUT_OF_STOCK_NOTIFICATION -> """
                We are sorry, but the product you ordered is currently out of stock.
                Your order has been cancelled and you will not be charged.
                Please check back later or explore similar products.
                """;

            case PAYMENT_SUCCESS_NOTIFICATION -> """
                Great news! Your payment was successful and your order is confirmed.
                We are now preparing your order for dispatch.
                Thank you for shopping with us!
                """;

            case PAYMENT_FAILED_NOTIFICATION -> """
                Unfortunately, we were unable to process your payment after multiple attempts.
                Your order has been cancelled and no charges have been made.
                Please check your payment details and try placing the order again.
                """;

            default -> "";
        };
    }
}
