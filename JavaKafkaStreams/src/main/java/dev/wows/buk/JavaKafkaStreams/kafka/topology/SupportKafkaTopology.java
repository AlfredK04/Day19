package dev.wows.buk.JavaKafkaStreams.kafka.topology;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Grouped;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import dev.wows.buk.JavaKafkaStreams.kafka.topics.StreamTopics;

@Configuration
@EnableKafkaStreams
public class SupportKafkaTopology {
    @Bean
    KStream<String, String> uppercaseMessageTopic(StreamsBuilder builder) {

        KStream<String, String> message = getMessage(builder);

        message
            .mapValues(v -> v.toUpperCase())
            .to(
                StreamTopics.SUPPORT_MESSAGES_UPPERCASE,
                Produced.with(Serdes.String(), Serdes.String())
            );

        return message;
    }

    @Bean 
    KStream<String, String> wordFrequencyMessageTopic(StreamsBuilder builder) {
        KStream<String, String> message = getMessage(builder);
        message
            .flatMapValues(v -> Arrays.asList(v.split("\\W+")))
            .filter((k, v) -> v != null && !v.isBlank())
            .filter((k, v) -> {
                String word = v.toLowerCase();
                return !word.equals("the")
                    && !word.equals("a")
                    && !word.equals("and");
            })
            .groupBy(
                (k, word) -> word.toLowerCase(),
                Grouped.with(Serdes.String(), Serdes.String())
            )
            .count()
            .toStream()
            .map((word, c) -> KeyValue.pair(word, String.valueOf(c)))
            .to(
                StreamTopics.SUPPORT_WORD_FREQUENCY,
                Produced.with(Serdes.String(), Serdes.String())
            );
        return message;
    }

    @Bean 
    KStream<String, String> messageCountTopic(StreamsBuilder builder) {
    KStream<String, String> message = getMessage(builder);
    message
        .groupBy(
            (k, v) -> "messages",
            Grouped.with(Serdes.String(), Serdes.String())
        )
        .count()
        .toStream()
        .mapValues(String::valueOf)
        .to(
            StreamTopics.SUPPORT_MESSAGE_COUNT,
            Produced.with(Serdes.String(), Serdes.String())
        );
    return message;
}

    private KStream<String, String> getMessage(StreamsBuilder builder) {

        return builder
            .stream(
                StreamTopics.SUPPORT_MESSAGES,
                Consumed.with(Serdes.String(), Serdes.String())
            )
            .filter((k, v) -> v != null && !v.isBlank());
    }
}