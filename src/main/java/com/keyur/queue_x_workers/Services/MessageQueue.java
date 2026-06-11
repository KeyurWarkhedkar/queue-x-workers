package com.keyur.queue_x_workers.Services;

import com.keyur.queue_x_workers.DTOs.EventDto;

public interface MessageQueue {

    void publish(String queueName, EventDto event);

    EventDto consume(String queueName);
}
