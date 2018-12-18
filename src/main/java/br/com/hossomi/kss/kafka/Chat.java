package br.com.hossomi.kss.kafka;

import br.com.hossomi.kss.kafka.consumer.ListenableConsumer;
import br.com.hossomi.kss.kafka.consumer.ListenableScanner;
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
import java.util.concurrent.TimeUnit;

import static java.lang.Runtime.getRuntime;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
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

    private void start() throws Exception {
        ListenableScanner scanner = new ListenableScanner(new Scanner(System.in), this::sendMessage);
        System.out.print("Name: ");
        name = scanner.read();

        consumer = new ListenableConsumer<>(createConsumer(), this::printMessage);
        new Thread(consumer).start();

        producer = createProducer();
        sendMessage("Entered the room");
        scanner.readUntilQuit("quit");

        close();
    }

    private void printMessage(ConsumerRecord<String, String> rec) {
        System.out.printf("[%s] (%d) %s > %s\n",
                new Date(rec.timestamp()),
                rec.partition(),
                rec.key() != null ? rec.key() : "(unknown)",
                rec.value());
    }

    private Future<RecordMetadata> sendMessage(String line) {
        return producer.send(new ProducerRecord<>(TOPIC, name, line));
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
                .thenRun(() -> producer.close(1000, MILLISECONDS));
    }

    // ----------------------------------------

    private static <T> CompletableFuture<T> completable(Future<T> future) {
        return supplyAsync(() -> {
            try {
                return future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }, newSingleThreadExecutor());
    }

    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();

        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                chat.close().get();
                System.out.println("XABLAUAS");
            }
            catch (Exception e) {
                log.error("Chat did not close nicely", e);
            }
        }));

        chat.start();
    }
}
