package br.com.hossomi.kss.kafka.chat.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofMillis;
import static java.util.stream.StreamSupport.stream;

@Slf4j
public class ListenableConsumer<K, V> implements Runnable, AutoCloseable {

    private final Consumer<K, V> consumer;
    private final RecordListener<K, V> listener;
    private final AtomicBoolean close = new AtomicBoolean(false);

    public ListenableConsumer(Consumer<K, V> consumer, RecordListener<K, V> listener) {
        this.consumer = consumer;
        this.listener = listener;
    }

    public void run() {
        try {
            while (!close.get()) {
                ConsumerRecords<K, V> records = consumer.poll(ofMillis(1000));
                log.debug("Received {} records", records.count());

                stream(records.spliterator(), false)
                        .sorted((rec1, rec2) -> (int) (rec1.timestamp() - rec2.timestamp()))
                        .forEach(this::notifyListener);
            }
        }
        catch (WakeupException e) {
            if (!close.get()) throw e;
        }
        finally {
            log.debug("Closing consumer");
            consumer.close();
        }
    }

    private void notifyListener(ConsumerRecord<K, V> record) {
        try {
            listener.onRecord(record);
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            consumer.commitSync(map);
        }
        catch (Exception e) {
            System.out.println("Error processing message");
        }
    }

    public void close() {
        close.set(true);
        consumer.wakeup();
    }
}
