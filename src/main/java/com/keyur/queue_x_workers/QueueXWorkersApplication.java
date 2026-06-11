package com.keyur.queue_x_workers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class QueueXWorkersApplication {

	public static void main(String[] args) {
		SpringApplication.run(QueueXWorkersApplication.class, args);
	}

}
