package org.folio.inventory.consortium.consumers;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.SimpleKafkaProducerManager;

public class SimpleKafkaProducer<K, V> {

  public KafkaProducer<K, V> create(Vertx vertx, KafkaConfig kafkaConfig, String topicName) {
    return new SimpleKafkaProducerManager(vertx, kafkaConfig).createShared(topicName);
  }
}
