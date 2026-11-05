/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.blazartech.kafkademo.data;

import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

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
        } catch (JacksonException ex) {
            throw new RuntimeException("error building JSON: " + ex.getMessage(), ex);
        }
    }
    
}
