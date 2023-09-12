package com.kafkaproject.listener

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.kafkaproject.model.Product
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Component
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

@Component
class ProductListener {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${kafka.topics.product}"], groupId = "ppr")
    @RetryableTopic(
            attempts = "5",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            autoCreateTopics = "false",
            backoff = Backoff(delay = 1000, multiplier = 2.0),
            exclude = [NullPointerException::class, DeserializationException::class]

    )
    fun topicListener(@Payload product: Product, ack: Acknowledgment) {
        logger.info("Message received {}", product)
        logger.info(product.name)
        logger.info(product.description)
        ack.acknowledge()
    }

    @DltHandler
    fun handleDltEvents(event: Product, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String){
        logger.info("Message: $event handled by dlq topic: $topic")
        val send_msg_request = SendMessageRequest()
                .withQueueUrl("https://sqs.us-east-1.amazonaws.com/517322566911/kafta_test_dlq")
                .withMessageBody(event.toString())
                .withDelaySeconds(5)
        val sqs = createSQSClient()
        sqs?.sendMessage(send_msg_request)
    }

    fun createSQSClient(): AmazonSQS? {
        return AmazonSQSClientBuilder.standard()
                .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(
                    "accessKey",
                        "secretKey"
                )))
                .withRegion(Regions.US_EAST_1)
                .build();

    }
}