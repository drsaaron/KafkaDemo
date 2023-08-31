/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.consumer;

import com.blazartech.kafkademo.data.DemoData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 *
 * @author scott
 */
@Component
@Slf4j
public class KafkaConsumer {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private DemoData stringToData(String json) {
        try {
            return objectMapper.readValue(json, DemoData.class);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("error parsing json: " + ex.getMessage(), ex);
        }
    }
    
    @KafkaListener(topics = "${demo.topic}", groupId = "${demo.groupID}")
    public void listen(String message) {
        DemoData data = stringToData(message);
        log.info("got message {}", data);
    }
}
