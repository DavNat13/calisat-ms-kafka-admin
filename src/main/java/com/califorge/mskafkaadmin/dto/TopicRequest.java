package com.califorge.mskafkaadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

/**
 * Alta/actualizacion de topico. {@code configs} admite cualquier config de topico
 * (p.ej. retention.ms, cleanup.policy=delete|compact, min.insync.replicas).
 */
public record TopicRequest(
        @NotBlank String name,
        @Min(1) Integer partitions,
        @Min(1) Short replicationFactor,
        Map<String, String> configs) {

    public int partitionsOrDefault() {
        return partitions == null ? 3 : partitions;
    }

    public short replicationOrDefault() {
        return replicationFactor == null ? 1 : replicationFactor;
    }
}
