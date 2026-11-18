package com.califorge.mskafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/** Cliente administrativo de Kafka (AdminClient) con bootstrap por variable de entorno. */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", 10000);
        props.put("default.api.timeout.ms", 15000);
        return AdminClient.create(props);
    }
}
