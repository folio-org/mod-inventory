package org.folio.inventory.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.commons.collections4.list.UnmodifiableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Iterators.cycle;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

public class KafkaProducerManager {

  // number of producers to be created is equal to allocated thread pool
  private static final int NUMBER_OF_PRODUCERS =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("event.publishing.thread.pool.size", "20"));
  private static Iterator<KafkaProducer<String, String>> producerIterator;

  public static void init(Vertx vertx, KafkaConfig config) {
    List<KafkaProducer<String, String>> producers = new ArrayList<>(NUMBER_OF_PRODUCERS);
    for (int i = 0; i < NUMBER_OF_PRODUCERS; i++) {
      producers.add(KafkaProducer.<String, String>create(vertx, config.getProducerProps()));
    }
    producerIterator = cycle(new UnmodifiableList<>(producers));
  }

  public static KafkaProducer<String, String> getKafkaProducer() {
    return Stream.generate(producerIterator::next)
      .filter(producer -> !producer.writeQueueFull())
      .findAny()
      .orElseThrow(() -> new RuntimeException("No eligible Kafka Producer available"));
  }
}
