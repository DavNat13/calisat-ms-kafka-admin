package com.califorge.mskafkaadmin.dto;

import jakarta.validation.constraints.Min;

import java.util.Map;

/** Modificacion de un topico existente: ampliar particiones y/o cambiar configs. */
public record TopicUpdateRequest(
        @Min(1) Integer partitions,
        Map<String, String> configs) {
}
