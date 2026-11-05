/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.consumer;

import com.blazartech.kafkademo.data.DemoData;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

/**
 *
 * @author scott
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {
    
    @Value("${kafka.bootstrapAddress}")
    private String bootstrapAddress;
    
    @Value("${demo.groupID}")
    private String groupID;
    
    @Bean
    public ConsumerFactory<String, DemoData> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.blazartech.kafkademo.data");
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    // The method name MUST be 'kafkaListenerContainerFactory' 
    // or you must explicitly provide the name via @Bean("kafkaListenerContainerFactory")
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DemoData> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DemoData> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
