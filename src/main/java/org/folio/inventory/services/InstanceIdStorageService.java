package org.folio.inventory.services;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;

public class InstanceIdStorageService implements IdStorageService {
  private static final Logger LOGGER = LogManager.getLogger(InstanceIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  public InstanceIdStorageService(EntityIdStorageDao entityIdStorageDao) {
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<RecordToEntity> store(String recordId, String instanceId, String tenantId) {
    RecordToEntity recordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId).build();
    LOGGER.info("Saving RecordToInstance relationship: {}", recordToInstance);
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToInstance, tenantId)
      .map(recordToInstance);
  }
}
