package org.folio.inventory.services;

import org.folio.kafka.services.KafkaTopic;

public class InventoryKafkaTopicService {

  public KafkaTopic[] createTopicObjects() {
    var instanceCreatedReadyForPostProcessing = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING",
      instanceCreatedReadyForPostProcessingNumPartitions());
    var instanceCreated = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_CREATED", instanceCreatedNumPartitions());
    var holdingCreated = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_CREATED", holdingCreatedNumPartitions());
    var itemCreated = new InventoryKafkaTopic("DI_INVENTORY_ITEM_CREATED", itemCreatedNumPartitions());

    var instanceMatched = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_MATCHED", instanceMatchedNumPartitions());
    var holdingMatched = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_MATCHED", holdingMatchedNumPartitions());
    var itemMatched = new InventoryKafkaTopic("DI_INVENTORY_ITEM_MATCHED", itemMatchedNumPartitions());

    var instanceUpdated = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_UPDATED", instanceUpdatedNumPartitions());
    var holdingUpdated = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_UPDATED", holdingUpdatedNumPartitions());
    var itemUpdated = new InventoryKafkaTopic("DI_INVENTORY_ITEM_UPDATED", itemUpdatedNumPartitions());

    var instanceNotMatched = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_NOT_MATCHED", instanceNotMatchedNumPartitions());

    var authorityUpdated = new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_UPDATED", authorityUpdatedNumPartitions());

    return new InventoryKafkaTopic[] {instanceCreatedReadyForPostProcessing, instanceCreated, holdingCreated,
                                      itemCreated, instanceMatched, holdingMatched, itemMatched,
                                      instanceUpdated, holdingUpdated, itemUpdated, instanceNotMatched, authorityUpdated};
  }

  private Integer instanceCreatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.instanceCreated.partitions", "1"));
  }

  private Integer holdingCreatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.holdingCreated.partitions", "1"));
  }

  private Integer instanceCreatedReadyForPostProcessingNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.instanceCreatedReadyForPostProcessing.partitions", "1"));
  }

  private Integer itemCreatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.itemCreated.partitions", "1"));
  }

  private Integer instanceMatchedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.instanceMatched.partitions", "1"));
  }

  private Integer holdingMatchedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.holdingMatched.partitions", "1"));
  }

  private Integer itemMatchedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.itemMatched.partitions", "1"));
  }

  private Integer instanceUpdatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.instanceUpdated.partitions", "1"));
  }

  private Integer holdingUpdatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.holdingUpdated.partitions", "1"));
  }

  private Integer itemUpdatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.itemUpdated.partitions", "1"));
  }

  private Integer instanceNotMatchedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.instanceNotMatched.partitions", "1"));
  }

  private Integer authorityUpdatedNumPartitions() {
    return Integer.valueOf(System.getProperty("inventory.kafka.authorityUpdated.partitions", "1"));
  }
}
