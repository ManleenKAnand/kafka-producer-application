package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerApplication {

  private final Producer<String, String> producer;
  private String outputTopic;

  public KafkaProducerApplication(final Producer<String, String> producer, final String topic) {
    this.producer = producer;
    outputTopic = topic;
  }

  public Future<RecordMetadata> produce(final String message) {
    final String[] parts = message.split("-");
    final String key, value;
    if (parts.length > 1) {
      key = parts[0];
      value = parts[1];
    } else {
      key = "NO-KEY";
      value = parts[0];
    }
    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(outputTopic, key, value);
    return producer.send(producerRecord);
  }

  public void shutdown() {
    producer.close();
  }

  public static Properties loadProperties(String fileName) throws IOException {
    final Properties properties = new Properties();
    final FileInputStream input = new FileInputStream(fileName);
    properties.load(input);
    input.close();
    return properties;
  }

  public void printMetadata(
      final Collection<Future<RecordMetadata>> metadata, final String fileName) {
    System.out.println("Offsets and timestamps committed in batch from " + fileName);
    metadata.forEach(
        m -> {
          try {
            final RecordMetadata recordMetadata = m.get();
            System.out.println(
                "Record written to offset: "
                    + recordMetadata.offset()
                    + " timestamp: "
                    + recordMetadata.timestamp());
          } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        });
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      throw new IllegalArgumentException(
          "This program takes two arguments: the path to an environment configuration file and"
              + "the path to the file with records to send");
    }

    final Properties properties = KafkaProducerApplication.loadProperties(args[0]);
    final String topic = properties.getProperty("output.topic.name");
    final Producer<String, String> producer = new KafkaProducer<String, String>(properties);
    final KafkaProducerApplication producerApplication =
        new KafkaProducerApplication(producer, topic);

    String filePath = args[1];
    try {
      List<String> linesToProduce = Files.readAllLines(Paths.get(filePath));
      List<Future<RecordMetadata>> metadata =
          linesToProduce.stream()
              .filter(l -> !l.trim().isEmpty())
              .map(producerApplication::produce)
              .collect(Collectors.toList());
      producerApplication.printMetadata(metadata, filePath);
    } catch (IOException e) {
      System.err.printf("Error reading file %s due to %s %n", filePath, e);
    } finally {
      producerApplication.shutdown();
    }
  }
}
