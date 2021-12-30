package org.folio.inventory.services;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;

public class ItemIdStorageService implements IdStorageService {
  private static final Logger LOGGER = LogManager.getLogger(ItemIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  public ItemIdStorageService(EntityIdStorageDao entityIdStorageDao){
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<RecordToEntity> store(String recordId, String itemId, String tenantId) {
    RecordToEntity recordToItem = RecordToEntity.builder().table(EntityTable.ITEM).recordId(recordId).entityId(itemId).build();
    LOGGER.info("Saving RecordToItem relationship: {}", recordToItem);
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToItem, tenantId);
  }
}
