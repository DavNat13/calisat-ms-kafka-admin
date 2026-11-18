package com.califorge.mskafkaadmin;

import com.califorge.mskafkaadmin.dto.TopicRequest;
import com.califorge.mskafkaadmin.dto.TopicUpdateRequest;
import com.califorge.mskafkaadmin.service.KafkaAdminException;
import com.califorge.mskafkaadmin.service.TopicAdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicAdminServiceTest {

    private final AdminClient admin = mock(AdminClient.class);
    private final TopicAdminService service = new TopicAdminService(admin);

    @Test
    @SuppressWarnings("unchecked")
    void crearTopicoEnviaParticionesReplicasYConfigs() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(any(Collection.class))).thenReturn(result);

        service.crear(new TopicRequest("interacciones-user", 3, (short) 1,
                Map.of("retention.ms", "604800000", "cleanup.policy", "delete")));

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic nt = captor.getValue().iterator().next();
        assertEquals("interacciones-user", nt.name());
        assertEquals(3, nt.numPartitions());
        assertEquals((short) 1, nt.replicationFactor());
        assertEquals("delete", nt.configs().get("cleanup.policy"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void crearTopicoExistenteLanzaKafkaAdminException() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        KafkaFuture<Void> fallo = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        ((org.apache.kafka.common.internals.KafkaFutureImpl<Void>) fallo)
                .completeExceptionally(new TopicExistsException("ya existe"));
        when(result.all()).thenReturn(fallo);
        when(admin.createTopics(any(Collection.class))).thenReturn(result);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> service.crear(new TopicRequest("t", 1, (short) 1, null)));
        assertInstanceOf(TopicExistsException.class, ex.causaOriginal());
    }

    @Test
    void listarDevuelveNombresOrdenados() {
        ListTopicsResult result = mock(ListTopicsResult.class);
        when(result.names()).thenReturn(KafkaFuture.completedFuture(Set.of("b", "a")));
        when(admin.listTopics()).thenReturn(result);
        assertEquals(List.of("a", "b"), List.copyOf(service.listar()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void noPermiteReducirParticiones() {
        Node n = new Node(1, "localhost", 9092);
        TopicDescription desc = new TopicDescription("t", false, List.of(
                new TopicPartitionInfo(0, n, List.of(n), List.of(n)),
                new TopicPartitionInfo(1, n, List.of(n), List.of(n)),
                new TopicPartitionInfo(2, n, List.of(n), List.of(n))));
        DescribeTopicsResult dr = mock(DescribeTopicsResult.class);
        when(dr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("t", desc)));
        when(admin.describeTopics(any(Collection.class))).thenReturn(dr);

        assertThrows(IllegalArgumentException.class,
                () -> service.actualizar("t", new TopicUpdateRequest(2, null)));
        verify(admin, never()).createPartitions(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void eliminarInvocaDeleteTopics() {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteTopics(any(Collection.class))).thenReturn(result);
        service.eliminar("t");
        verify(admin).deleteTopics(List.of("t"));
    }
}
