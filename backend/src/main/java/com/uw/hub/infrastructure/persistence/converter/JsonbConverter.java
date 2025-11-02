package com.uw.hub.infrastructure.persistence.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/**
 * JPA AttributeConverter for handling JSONB columns in PostgreSQL.
 * Converts between Map<String, Object> and JSON string representation.
 */
@Converter
@Slf4j
public class JsonbConverter implements AttributeConverter<Map<String, Object>, String> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
        new TypeReference<Map<String, Object>>() {};

    @Override
    public String convertToDatabaseColumn(Map<String, Object> attribute) {
        if (attribute == null) {
            return null;
        }

        try {
            String json = objectMapper.writeValueAsString(attribute);
            log.trace("Converted Map to JSON: {}", json);
            return json;
        } catch (JsonProcessingException e) {
            log.error("Failed to convert Map to JSON", e);
            throw new IllegalArgumentException("Error converting Map to JSON", e);
        }
    }

    @Override
    public Map<String, Object> convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isEmpty()) {
            return null;
        }

        try {
            Map<String, Object> map = objectMapper.readValue(dbData, MAP_TYPE_REF);
            log.trace("Converted JSON to Map: {}", map);
            return map;
        } catch (IOException e) {
            log.error("Failed to convert JSON to Map: {}", dbData, e);
            throw new IllegalArgumentException("Error converting JSON to Map", e);
        }
    }
}
