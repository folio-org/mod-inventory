package org.folio.inventory.services;

import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;

import org.folio.kafka.services.KafkaTopic;

public class InventoryKafkaTopic implements KafkaTopic {

  private final String topic;
  private final int numPartitions;

  public InventoryKafkaTopic(String topic, int numPartitions) {
    this.topic = topic;
    this.numPartitions = numPartitions;
  }

  @Override
  public String moduleName() {
    return "inventory";
  }

  @Override
  public String topicName() {
    return topic;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public String fullTopicName(String tenant) {
    return formatTopicName(environment(), getDefaultNameSpace(), tenant, topicName());
  }
}
