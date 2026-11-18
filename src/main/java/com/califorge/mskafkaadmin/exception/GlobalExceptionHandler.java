package com.califorge.mskafkaadmin.exception;

import com.califorge.mskafkaadmin.service.KafkaAdminException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> validacion(MethodArgumentNotValidException e) {
        return ResponseEntity.badRequest().body(Map.of("error", "Solicitud invalida"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> argumento(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> estado(IllegalStateException e) {
        return ResponseEntity.status(409).body(Map.of("error", String.valueOf(e.getMessage())));
    }

    @ExceptionHandler(KafkaAdminException.class)
    public ResponseEntity<Map<String, String>> kafka(KafkaAdminException e) {
        Throwable causa = e.causaOriginal();
        if (causa instanceof TopicExistsException) {
            return ResponseEntity.status(409).body(Map.of("error", "El topico ya existe"));
        }
        if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
            return ResponseEntity.status(404).body(Map.of("error", "Recurso Kafka no encontrado"));
        }
        log.error("Error de Kafka: {}", e.getMessage());
        return ResponseEntity.status(502).body(Map.of("error", String.valueOf(e.getMessage())));
    }
}
