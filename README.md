# calisat-ms-kafka-admin

Administrador de Apache Kafka (AdminClient) — puerto **8091** · Java 21 · Spring Boot 4.1.0

## Endpoints (JWT Entra/Cognito)
```
POST/GET /api/v1/kafka/topics · GET/PUT/DELETE /api/v1/kafka/topics/{name} (PUT: ampliar particiones + configs retention.ms/cleanup.policy)
GET /api/v1/kafka/groups · GET/DELETE /api/v1/kafka/groups/{id} · POST /api/v1/kafka/groups/{id}/reset-offsets {topic,to:earliest|latest}
```

## Variables de entorno
SPRING_KAFKA_BOOTSTRAP_SERVERS

## Ejecutar
```
./mvnw clean test
docker compose up -d --build   # requiere calisat-infra arriba (red calisat-net)
```
