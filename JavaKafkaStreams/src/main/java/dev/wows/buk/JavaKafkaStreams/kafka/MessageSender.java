package dev.wows.buk.JavaKafkaStreams.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import dev.wows.buk.JavaKafkaStreams.kafka.topics.StreamTopics;

@Service
public class MessageSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String sendMessage(String message) {

        kafkaTemplate.send(StreamTopics.SUPPORT_MESSAGES, message);

        return "sent: " + message;
    }
}