package com.keyur.queue_x_workers.Services;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueueConstants {
    public static final String orderQueue = "order_queue";
    public static final String outOfStock = "out_of_stock";
    public static final String notification = "notification";
    public static final String paymentQueue = "payment_queue";
    public static final String paymentSuccessQueue = "payment_success_queue";
    public static final String paymentFailedQueue = "payment_failed_queue";
    public static final String paymentFailedForOrderApiQueue = "payment_failed_for_order_api_queue";
}
