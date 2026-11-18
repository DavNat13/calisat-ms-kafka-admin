package com.califorge.mskafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Reinicio de offsets de un grupo (el grupo debe estar sin miembros activos). */
public record ResetOffsetsRequest(
        @NotBlank String topic,
        @NotBlank @Pattern(regexp = "earliest|latest") String to) {
}
