/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.producer;

import com.blazartech.kafkademo.data.DemoData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 *
 * @author scott
 */
@Component
@Order(1)
@Slf4j
public class ProducerCommandLineRunner implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Value("${demo.topic}")
    private String demoTopic;
    
    @Value("${demo.message}")
    private String demoMessage;
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private String dataToString(DemoData d) {
        try {
            return objectMapper.writeValueAsString(d);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("error converting " + d + " to json: " + ex.getMessage(), ex);
        }
    }
    
    @Override
    public void run(String... args) throws Exception {
        log.info("sending messages");
        
        DemoData[] data = {
            new DemoData(1, "Scott", 25),
            new DemoData(2, "EVH", 67),
            new DemoData(3, "Clapton", 78),
            new DemoData(4, "Page", 79),
            new DemoData(5, "Beck", 80)
        };
        
        Collection<CompletableFuture<SendResult<String, String>>> futures = Arrays.asList(data).stream()
                .map(d -> dataToString(d))
                .map(d -> kafkaTemplate.send(demoTopic, d))
                .collect(Collectors.toList());
        
        BiConsumer<SendResult<String, String>, Throwable> handler = (result, ex) -> {
            if (ex != null) {
               log.error("unable to send message: " + ex.getMessage(), ex);
           } else {
               log.info("sent message = [" + result.getProducerRecord().value() + "] with offset [" + result.getRecordMetadata().offset() + "]");
           }
        };
        
        futures.stream().forEach(f -> f.whenComplete(handler));
        
    }
    
}
