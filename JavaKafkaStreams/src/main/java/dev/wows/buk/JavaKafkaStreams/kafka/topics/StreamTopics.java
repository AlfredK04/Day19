package dev.wows.buk.JavaKafkaStreams.kafka.topics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class StreamTopics {

    public static final String
        SUPPORT_MESSAGES = "support-messages",
        SUPPORT_MESSAGES_UPPERCASE = "support-messages-uppercase",
        SUPPORT_MESSAGE_COUNT = "support-message-count",
        SUPPORT_WORD_FREQUENCY = "support-word-frequency";

    @Bean
    NewTopic supportMessageTopic() {

        return TopicBuilder
                .name(SUPPORT_MESSAGES)
                .partitions(1)
                .replicas(1)
            .build();
    }

    @Bean
    NewTopic supportMessageUppercateTopic() {

        return TopicBuilder
                .name(SUPPORT_MESSAGES_UPPERCASE)
                .partitions(1)
                .replicas(1)
            .build();
    }

    @Bean 
    NewTopic supportMessageCountTopic() {
        return  TopicBuilder
                .name(SUPPORT_MESSAGE_COUNT)
                .partitions(1)
                .replicas(1)
            .build();
    }

    @Bean 
    NewTopic supportWordFrequencyTopic() {
        return TopicBuilder
                .name(SUPPORT_WORD_FREQUENCY)
                .partitions(1)
                .replicas(1)
                .compact()
            .build();
    }
}