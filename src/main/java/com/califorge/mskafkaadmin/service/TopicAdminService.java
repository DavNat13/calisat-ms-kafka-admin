package com.califorge.mskafkaadmin.service;

import com.califorge.mskafkaadmin.dto.TopicRequest;
import com.califorge.mskafkaadmin.dto.TopicUpdateRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/** Administracion de topicos: crear, listar, describir, actualizar y eliminar. */
@Service
public class TopicAdminService {

    private static final Logger log = LoggerFactory.getLogger(TopicAdminService.class);

    private final AdminClient admin;

    public TopicAdminService(AdminClient admin) {
        this.admin = admin;
    }

    public void crear(TopicRequest req) {
        NewTopic topic = new NewTopic(req.name(), req.partitionsOrDefault(), req.replicationOrDefault());
        if (req.configs() != null && !req.configs().isEmpty()) {
            topic.configs(req.configs());
        }
        esperar(() -> admin.createTopics(List.of(topic)).all());
        log.info("Topico creado: {} (particiones={}, replicas={}, configs={})",
                req.name(), req.partitionsOrDefault(), req.replicationOrDefault(), req.configs());
    }

    public Set<String> listar() {
        return new TreeSet<>(esperar(() -> admin.listTopics().names()));
    }

    public Map<String, Object> describir(String nombre) {
        TopicDescription desc = esperar(() -> admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
        Config cfg = esperar(() -> admin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, nombre))).all())
                .values().iterator().next();
        Map<String, String> configs = new LinkedHashMap<>();
        for (String clave : List.of("retention.ms", "cleanup.policy", "min.insync.replicas", "segment.bytes",
                "max.message.bytes")) {
            ConfigEntry e = cfg.get(clave);
            if (e != null) {
                configs.put(clave, e.value());
            }
        }
        List<Map<String, Object>> particiones = new ArrayList<>();
        desc.partitions().forEach(p -> particiones.add(Map.of(
                "partition", p.partition(),
                "leader", p.leader() == null ? -1 : p.leader().id(),
                "replicas", p.replicas().size(),
                "isr", p.isr().size())));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", desc.name());
        out.put("internal", desc.isInternal());
        out.put("partitions", particiones);
        out.put("configs", configs);
        return out;
    }

    /** Amplia particiones (Kafka no permite reducirlas) y altera configs de forma incremental. */
    public void actualizar(String nombre, TopicUpdateRequest req) {
        if (req.partitions() != null) {
            int actuales = esperar(() -> admin.describeTopics(List.of(nombre)).allTopicNames())
                    .get(nombre).partitions().size();
            if (req.partitions() < actuales) {
                throw new IllegalArgumentException("Kafka no permite reducir particiones (" + actuales
                        + " -> " + req.partitions() + ")");
            }
            if (req.partitions() > actuales) {
                esperar(() -> admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(req.partitions()))).all());
            }
        }
        if (req.configs() != null && !req.configs().isEmpty()) {
            Collection<AlterConfigOp> ops = new ArrayList<>();
            req.configs().forEach((k, v) -> ops.add(
                    new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
            ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            esperar(() -> admin.incrementalAlterConfigs(Map.of(recurso, ops)).all());
        }
        log.info("Topico actualizado: {} (particiones={}, configs={})", nombre, req.partitions(), req.configs());
    }

    public void eliminar(String nombre) {
        esperar(() -> admin.deleteTopics(List.of(nombre)).all());
        log.info("Topico eliminado: {}", nombre);
    }

    static <T> T esperar(java.util.function.Supplier<org.apache.kafka.common.KafkaFuture<T>> op) {
        try {
            return op.get().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("Operacion interrumpida", e);
        } catch (ExecutionException e) {
            throw new KafkaAdminException(e.getCause() == null ? e.getMessage() : e.getCause().getMessage(),
                    e.getCause() == null ? e : e.getCause());
        }
    }
}
