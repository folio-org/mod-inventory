package org.folio.inventory.services;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;

public class HoldingsIdStorageService implements IdStorageService{
  private static final Logger LOGGER = LogManager.getLogger(HoldingsIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  public HoldingsIdStorageService(EntityIdStorageDao entityIdStorageDao) {
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<RecordToEntity> store(String recordId, String instanceId, String tenantId) {
    RecordToEntity recordToHoldings = RecordToEntity.builder().table(EntityTable.HOLDINGS).recordId(recordId).entityId(instanceId).build();
    LOGGER.info("Saving RecordToHoldings relationship: {}", recordToHoldings);
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToHoldings, tenantId);
  }
}
