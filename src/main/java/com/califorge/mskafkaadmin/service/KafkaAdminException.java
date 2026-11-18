package com.califorge.mskafkaadmin.service;

/** Error devuelto por el broker Kafka durante una operacion administrativa. */
public class KafkaAdminException extends RuntimeException {

    private final Throwable causaOriginal;

    public KafkaAdminException(String mensaje, Throwable causa) {
        super(mensaje, causa);
        this.causaOriginal = causa;
    }

    public Throwable causaOriginal() {
        return causaOriginal;
    }
}
