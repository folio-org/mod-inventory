package org.folio.inventory.services;

import static org.apache.commons.lang3.StringUtils.firstNonBlank;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.inventory.InstanceIngressConsumerVerticle.INSTANCE_INGRESS_TOPIC;

import org.folio.kafka.services.KafkaTopic;

public class InventoryKafkaTopicService {

  public KafkaTopic[] createTopicObjects() {
    return new InventoryKafkaTopic[] {
      new InventoryKafkaTopic(DI_INVENTORY_INSTANCE_CREATED.value(), instanceCreatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_HOLDING_CREATED.value(), holdingCreatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_ITEM_CREATED.value(), itemCreatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_INSTANCE_MATCHED.value(), instanceMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_HOLDING_MATCHED.value(), holdingMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_ITEM_MATCHED.value(), itemMatchedPartitions()),
      new InventoryKafkaTopic(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), marcBibMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_INSTANCE_UPDATED.value(), instanceUpdatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_HOLDING_UPDATED.value(), holdingUpdatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), instanceNotMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_HOLDING_NOT_MATCHED.value(), holdingNotMatchedPartitions()),
      new InventoryKafkaTopic(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), marcBibNotMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_ITEM_UPDATED.value(), itemUpdatedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_ITEM_NOT_MATCHED.value(), itemNotMatchedPartitions()),
      new InventoryKafkaTopic(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value(), holdingCreatedReadyForPostProcessingPartitions()),
      new InventoryKafkaTopic(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), marcBibRecordModifiedPartitions()),
      new InventoryKafkaTopic(INSTANCE_INGRESS_TOPIC, instanceIngressPartitions())
    };
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

  private Integer marcBibMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_SRS_MARC_BIB_RECORD_MATCHED_PARTITIONS"), "1"));
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

  private Integer marcBibNotMatchedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED_PARTITIONS"), "1"));
  }

  private Integer holdingCreatedReadyForPostProcessingPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv(
      "DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING_PARTITIONS"), "1"));
  }

  private Integer marcBibRecordModifiedPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv(
      "DI_SRS_MARC_BIB_RECORD_MODIFIED_PARTITIONS"), "1"));
  }

  private Integer instanceIngressPartitions() {
    return Integer.valueOf(firstNonBlank(System.getenv("INVENTORY_INSTANCE_INGRESS_PARTITIONS"), "1"));
  }
}
