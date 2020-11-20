package io.confluent.developer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

public class KafkaProducerApplicationTest {

  private static final String TEST_CONFIG = "configuration/test.properties";

  @Test
  public void testProduce() throws IOException {
    final MockProducer<String, String> mockProducer = new MockProducer<>();
    final Properties properties = KafkaProducerApplication.loadProperties(TEST_CONFIG);
    final String topic = properties.getProperty("output.topic.name");
    KafkaProducerApplication producerApplication =
        new KafkaProducerApplication(mockProducer, topic);
    final List<String> records =
        Arrays.asList("abc-xyz", "mouse-keyboard", "pencil-eraser", "screen-wire");

    records.forEach(producerApplication::produce);

    final List<KeyValue<String, String>> expectedList =
        Arrays.asList(
            KeyValue.pair("abc", "xyz"),
            KeyValue.pair("mouse", "keyboard"),
            KeyValue.pair("pencil", "eraser"),
            KeyValue.pair("screen", "wire"));

    final List<KeyValue<String, String>> actualList =
        mockProducer.history().stream().map(this::toKeyValue).collect(Collectors.toList());

    assertThat(actualList, equalTo(expectedList));
    producerApplication.shutdown();
  }

  private KeyValue<String, String> toKeyValue(final ProducerRecord<String, String> producerRecord) {
    return KeyValue.pair(producerRecord.key(), producerRecord.value());
  }
}
