package com.califorge.mskafkaadmin.controller;

import com.califorge.mskafkaadmin.dto.TopicRequest;
import com.califorge.mskafkaadmin.dto.TopicUpdateRequest;
import com.califorge.mskafkaadmin.service.TopicAdminService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

/** API de administracion de Kafka: topicos (rol ADMINISTRADOR). */
@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaAdminController {

    private final TopicAdminService topics;

    public KafkaAdminController(TopicAdminService topics) {
        this.topics = topics;
    }

    // ---- Topicos ----
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> crearTopico(@Valid @RequestBody TopicRequest req) {
        topics.crear(req);
        return ResponseEntity.status(201).body(Map.of("topic", req.name(), "estado", "CREADO"));
    }

    @GetMapping("/topics")
    public Set<String> listarTopicos() {
        return topics.listar();
    }

    @GetMapping("/topics/{name}")
    public Map<String, Object> describirTopico(@PathVariable String name) {
        return topics.describir(name);
    }

    @PutMapping("/topics/{name}")
    public Map<String, Object> actualizarTopico(@PathVariable String name,
                                                @Valid @RequestBody TopicUpdateRequest req) {
        topics.actualizar(name, req);
        return topics.describir(name);
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> eliminarTopico(@PathVariable String name) {
        topics.eliminar(name);
        return ResponseEntity.noContent().build();
    }
}
