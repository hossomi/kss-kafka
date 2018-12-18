package br.com.hossomi.kss.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

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
                        .forEach(listener::onRecord);
            }
        }
        catch (WakeupException e) {
            if (!close.get()) throw e;
        }
        finally {
            log.debug("Closing consumer");
            consumer.close();
        }

        System.out.println("EXITED");
    }

    public void close() {
        close.set(true);
        consumer.wakeup();
    }
}
