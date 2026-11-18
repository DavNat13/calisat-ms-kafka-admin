package com.califorge.mskafkaadmin.controller;

import com.califorge.mskafkaadmin.dto.ResetOffsetsRequest;
import com.califorge.mskafkaadmin.dto.TopicRequest;
import com.califorge.mskafkaadmin.dto.TopicUpdateRequest;
import com.califorge.mskafkaadmin.service.ConsumerGroupAdminService;
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

import java.util.List;
import java.util.Map;
import java.util.Set;

/** API de administracion de Kafka: topicos y consumer groups (rol ADMINISTRADOR). */
@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaAdminController {

    private final TopicAdminService topics;
    private final ConsumerGroupAdminService groups;

    public KafkaAdminController(TopicAdminService topics, ConsumerGroupAdminService groups) {
        this.topics = topics;
        this.groups = groups;
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

    // ---- Consumer groups ----
    @GetMapping("/groups")
    public List<Map<String, Object>> listarGrupos() {
        return groups.listar();
    }

    @GetMapping("/groups/{groupId}")
    public Map<String, Object> describirGrupo(@PathVariable String groupId) {
        return groups.describir(groupId);
    }

    @PostMapping("/groups/{groupId}/reset-offsets")
    public ResponseEntity<Map<String, String>> resetearOffsets(@PathVariable String groupId,
                                                               @Valid @RequestBody ResetOffsetsRequest req) {
        groups.resetearOffsets(groupId, req.topic(), req.to());
        return ResponseEntity.ok(Map.of("groupId", groupId, "topic", req.topic(), "to", req.to()));
    }

    @DeleteMapping("/groups/{groupId}")
    public ResponseEntity<Void> eliminarGrupo(@PathVariable String groupId) {
        groups.eliminar(groupId);
        return ResponseEntity.noContent().build();
    }
}
