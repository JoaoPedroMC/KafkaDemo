package com.kafkaproject.listener

import com.kafkaproject.model.Product
import org.springframework.stereotype.Component
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import java.lang.Thread.sleep

@Component
class ProductListener {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${kafka.topics.product}"], groupId = "ppr")
    fun topicListener(@Payload product: Product, ack: Acknowledgment) {
        logger.info("Message received {}", product)
        logger.info(product.name)
        logger.info(product.description)
        ack.acknowledge()
    }
}