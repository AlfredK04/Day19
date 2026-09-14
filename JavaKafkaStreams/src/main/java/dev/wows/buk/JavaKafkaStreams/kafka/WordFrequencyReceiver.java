package dev.wows.buk.JavaKafkaStreams.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

import dev.wows.buk.JavaKafkaStreams.kafka.topics.StreamTopics;

// Dedicated seek-aware component: only the word-frequency listener is rewound on assignment
@Component
public class WordFrequencyReceiver extends AbstractConsumerSeekAware {

    private final Map<String, Long> wordsFreq = new HashMap<>();

    @Override
    public void onPartitionsAssigned(
        Map<TopicPartition, Long> assignments,
        ConsumerSeekCallback callback
    ) {
        super.onPartitionsAssigned(assignments, callback);

        wordsFreq.clear();
        callback.seekToBeginning(assignments.keySet());
    }

    @KafkaListener(topics = StreamTopics.SUPPORT_WORD_FREQUENCY)
    public void readMessageWordFreq(ConsumerRecord<String, String> wordFreq) {

        wordsFreq.put(wordFreq.key(), Long.valueOf(wordFreq.value()));

        System.out.println(
            "Full words frequency: " + wordsFreq
        );
    }
}