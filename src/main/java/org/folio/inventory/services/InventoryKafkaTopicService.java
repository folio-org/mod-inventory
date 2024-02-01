package org.folio.inventory.services;

import static org.apache.commons.lang3.StringUtils.firstNonBlank;

import org.folio.kafka.services.KafkaTopic;

public class InventoryKafkaTopicService {

  public KafkaTopic[] createTopicObjects() {
    var instanceCreated = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_CREATED", instanceCreatedPartitions());
    var holdingCreated = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_CREATED", holdingCreatedPartitions());
    var itemCreated = new InventoryKafkaTopic("DI_INVENTORY_ITEM_CREATED", itemCreatedPartitions());

    var instanceMatched = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_MATCHED", instanceMatchedPartitions());
    var holdingMatched = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_MATCHED", holdingMatchedPartitions());
    var itemMatched = new InventoryKafkaTopic("DI_INVENTORY_ITEM_MATCHED", itemMatchedPartitions());

    var instanceUpdated = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_UPDATED", instanceUpdatedPartitions());
    var holdingUpdated = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_UPDATED", holdingUpdatedPartitions());
    var itemUpdated = new InventoryKafkaTopic("DI_INVENTORY_ITEM_UPDATED", itemUpdatedPartitions());

    var instanceNotMatched = new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_NOT_MATCHED", instanceNotMatchedPartitions());
    var holdingNotMatched = new InventoryKafkaTopic("DI_INVENTORY_HOLDING_NOT_MATCHED", holdingNotMatchedPartitions());
    var itemNotMatched = new InventoryKafkaTopic("DI_INVENTORY_ITEM_NOT_MATCHED", itemNotMatchedPartitions());
    var authorityUpdated = new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_UPDATED", authorityUpdatedPartitions());

    return new InventoryKafkaTopic[] {instanceCreated, holdingCreated,
                                      itemCreated, instanceMatched, holdingMatched, itemMatched,
                                      instanceUpdated, holdingUpdated, itemUpdated, instanceNotMatched,
                                      holdingNotMatched, itemNotMatched, authorityUpdated};
  }

  private Integer instanceCreatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_INSTANCE_CREATED_PARTITIONS"), "1"));
  }

  private Integer holdingCreatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_HOLDING_CREATED_PARTITIONS"), "1"));
  }

  private Integer itemCreatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_ITEM_CREATED_PARTITIONS"), "1"));
  }

  private Integer instanceMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_INSTANCE_MATCHED_PARTITIONS"), "1"));
  }

  private Integer holdingMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_HOLDING_MATCHED_PARTITIONS"), "1"));
  }

  private Integer itemMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_ITEM_MATCHED_PARTITIONS"), "1"));
  }

  private Integer instanceUpdatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_INSTANCE_UPDATED_PARTITIONS"), "1"));
  }

  private Integer holdingUpdatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_HOLDING_UPDATED_PARTITIONS"), "1"));
  }

  private Integer itemUpdatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_ITEM_UPDATED_PARTITIONS"), "1"));
  }

  private Integer instanceNotMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_INSTANCE_NOT_MATCHED_PARTITIONS"), "1"));
  }

  private Integer itemNotMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_HOLDING_NOT_MATCHED_PARTITIONS"), "1"));
  }

  private Integer holdingNotMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_ITEM_NOT_MATCHED_PARTITIONS"), "1"));
  }

  private Integer authorityUpdatedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_INVENTORY_AUTHORITY_UPDATED_PARTITIONS"), "1"));
  }
}
