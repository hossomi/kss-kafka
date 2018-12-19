package br.com.hossomi.kss.kafka.chat;

import br.com.hossomi.kss.kafka.chat.consumer.ListenableConsumer;
import br.com.hossomi.kss.kafka.chat.consumer.ListenableScanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class Chat {

    private static final String KAFKA_URL = "localhost:9092";
    private static final String TOPIC = "messages-2";

    private ListenableConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private String name;
    private boolean error = false;

    public void start() throws Exception {
        ListenableScanner scanner = new ListenableScanner(new Scanner(System.in), this::sendMessage);
        System.out.print("Name: ");
        name = scanner.read();

        consumer = new ListenableConsumer<>(createConsumer(name), this::printMessage);
        new Thread(consumer).start();

        producer = createProducer();
        sendMessage("Entered the room");
        scanner.readUntilQuit("quit");
    }

    private void printMessage(ConsumerRecord<String, String> rec) {
        if (rec.value().equalsIgnoreCase("error")) {
            error = !error;
            System.out.println("Error was set to " + error);
            return;
        }

        if (error) throw new IllegalArgumentException();
        System.out.printf("[%s] (%d) %s > %s\n",
                new Date(rec.timestamp()),
                rec.partition(),
                rec.key() != null ? rec.key() : "(unknown)",
                rec.value());
    }

    private Future<RecordMetadata> sendMessage(String line) {
        return producer.send(new ProducerRecord<>(TOPIC, name, line));
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(asList(TOPIC));
        return consumer;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public Future<?> close() {
        log.debug("Closing chat");
        consumer.close();
        return completable(sendMessage("Left the room"))
                .handle((x, e) -> {
                    producer.close(1000, MILLISECONDS);
                    return null;
                });
    }

    // ----------------------------------------

    private static <T> CompletableFuture<T> completable(Future<T> future) {
        return supplyAsync(() -> {
            try {
                return future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();
        chat.start();
        chat.close().get();
    }
}
