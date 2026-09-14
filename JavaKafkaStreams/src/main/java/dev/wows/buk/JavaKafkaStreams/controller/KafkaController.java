package dev.wows.buk.JavaKafkaStreams.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dev.wows.buk.JavaKafkaStreams.kafka.MessageSender;

@RestController
@RequestMapping("kafka/support")
public class KafkaController {

    private final MessageSender messageSender;

    public KafkaController(MessageSender messageSender) {
        this.messageSender = messageSender;
    }

    @GetMapping
    public void sendMessage(@RequestParam String message) {

        messageSender.sendMessage(message);
    }
}