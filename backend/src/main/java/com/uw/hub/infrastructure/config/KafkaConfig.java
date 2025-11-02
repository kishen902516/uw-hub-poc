package com.uw.hub.infrastructure.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for consuming CDC messages from Debezium.
 * Configures consumer factory with proper deserializers for JSON messages.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${spring.kafka.listener.concurrency:3}")
    private int concurrency;

    /**
     * Consumer factory configuration for Debezium CDC messages
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        // Bootstrap servers
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Consumer group
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserializers (Debezium sends JSON strings)
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset management
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit for reliability

        // Performance tuning
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Session and heartbeat
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Kafka listener container factory with manual acknowledgment
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true); // Enable batch processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    /**
     * Single-record listener container factory (for non-batch processing if needed)
     */
    @Bean(name = "singleRecordKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> singleRecordKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.setBatchListener(false); // Single record processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }
}
