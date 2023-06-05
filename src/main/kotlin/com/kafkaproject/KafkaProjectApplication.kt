package com.kafkaproject

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaProjectApplication

fun main(args: Array<String>) {
    runApplication<KafkaProjectApplication>(*args)
}
