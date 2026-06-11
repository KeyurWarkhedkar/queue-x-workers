package com.keyur.queue_x_workers.Services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.keyur.queue_x_workers.DTOs.EventDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisMessageQueue implements MessageQueue {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void publish(String queueName, EventDto event) {
        redisTemplate.opsForList()
                .rightPush(queueName, event);
    }

    @Override
    public EventDto consume(String queueName) {

        Object obj = redisTemplate.opsForList()
                .leftPop(queueName);

        if (obj == null) return null;

        return objectMapper.convertValue(obj, EventDto.class);
    }
}