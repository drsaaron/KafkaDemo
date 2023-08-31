/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author scott
 */
public class DemoDataSerializer implements Serializer<DemoData> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(String string, DemoData t) {
        String json;
        try {
            json = objectMapper.writeValueAsString(t);
            return json.getBytes();
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("error building JSON: " + ex.getMessage(), ex);
        }
    }
    
}
