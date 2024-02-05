package org.folio.inventory.services;

import static org.apache.commons.lang3.StringUtils.firstNonBlank;

import org.folio.kafka.services.KafkaTopic;

public class InventoryKafkaTopicService {

  public KafkaTopic[] createTopicObjects() {
    return new InventoryKafkaTopic[] {
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_CREATED", instanceCreatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_CREATED", holdingCreatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_CREATED", itemCreatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_MATCHED", instanceMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_MATCHED", holdingMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_MATCHED", itemMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_UPDATED", instanceUpdatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_UPDATED", holdingUpdatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_NOT_MATCHED", instanceNotMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_NOT_MATCHED", holdingNotMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_UPDATED", itemUpdatedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_NOT_MATCHED", itemNotMatchedPartitions()),
      new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_UPDATED", authorityUpdatedPartitions())};
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
