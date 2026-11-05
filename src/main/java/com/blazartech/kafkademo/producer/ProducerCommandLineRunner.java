/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.producer;

import com.blazartech.kafkademo.data.DemoData;
import java.util.Arrays;
import java.util.function.BiConsumer;
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
    private KafkaTemplate<String, DemoData> kafkaTemplate;
    
    @Value("${demo.topic}")
    private String demoTopic;
    
    @Value("${demo.message}")
    private String demoMessage;
    
    private static final BiConsumer<SendResult<String, DemoData>, Throwable> handler = (result, ex) -> {
            if (ex != null) {
               log.error("unable to send message: " + ex.getMessage(), ex);
           } else {
               log.info("sent message = [" + result.getProducerRecord().value() + "] with offset [" + result.getRecordMetadata().offset() + "]");
           }
        };
    
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
        
        Arrays.asList(data).stream()
                .map(d -> kafkaTemplate.send(demoTopic, d))
                .forEach(f -> f.whenComplete(handler));
        
    }
    
}
