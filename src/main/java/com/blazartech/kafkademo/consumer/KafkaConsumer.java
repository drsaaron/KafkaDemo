/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.consumer;

import com.blazartech.kafkademo.data.DemoData;
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
    
    @KafkaListener(topics = "${demo.topic}", groupId = "${demo.groupID}")
    public void listen(DemoData data) {
        log.info("got message {}", data);
    }
}
