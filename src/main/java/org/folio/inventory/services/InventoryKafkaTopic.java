package org.folio.inventory.services;

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
}
