package br.com.hossomi.kss.kafka.chat.exception;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RetriableException extends RuntimeException {

    public final ConsumerRecord<?, ?> record;

    public RetriableException(ConsumerRecord<?, ?> record) {
        this.record = record;
    }
}
