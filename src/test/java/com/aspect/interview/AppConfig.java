package com.aspect.interview;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
	@Bean
	AspectPriorityQueue queue() {
		return new AspectPriorityQueue();
	}
}
