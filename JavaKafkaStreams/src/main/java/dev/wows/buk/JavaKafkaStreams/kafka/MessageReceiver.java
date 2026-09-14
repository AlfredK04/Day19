package dev.wows.buk.JavaKafkaStreams.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.wows.buk.JavaKafkaStreams.kafka.topics.StreamTopics;

@Component
public class MessageReceiver {

    @KafkaListener(topics = StreamTopics.SUPPORT_MESSAGES)
    public void readMessage(String message) {

        System.out.println(
            "Plain message: " + message
        );
    }

    @KafkaListener(topics = StreamTopics.SUPPORT_MESSAGES_UPPERCASE)
    public void readMessageUppercase(String message) {

        System.out.println(
            "UPPERCASE <- " + message
        );
    }

    @KafkaListener(topics = StreamTopics.SUPPORT_MESSAGE_COUNT)
    public void readMessageWordCount(String count) {
        System.out.println(
            "MESSAGE COUNT <- " + count
        );
    }
}