package org.folio.inventory.services;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ItemIdStorageService implements IdStorageService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ItemIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  public ItemIdStorageService(EntityIdStorageDao entityIdStorageDao){
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<Optional<RecordToEntity>> store(String recordId, String itemId, String tenantId) {
    RecordToEntity recordToItem = RecordToEntity.builder().table(EntityTable.ITEM).recordId(recordId).entityId(itemId).build();
    LOGGER.info("Saving RecordToItem relationship: {}", recordToItem);
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToItem, tenantId)
      .map(Optional.of(recordToItem));
  }
}
