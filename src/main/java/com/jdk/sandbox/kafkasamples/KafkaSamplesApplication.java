package com.jdk.sandbox.kafkasamples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaSamplesApplication {

	static void main(String[] args) {
		SpringApplication.run(KafkaSamplesApplication.class, args);
	}

}
