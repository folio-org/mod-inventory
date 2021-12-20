package org.folio.inventory.services;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class InstanceIdStorageService implements IdStorageService{

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  @Autowired
  public InstanceIdStorageService(EntityIdStorageDao entityIdStorageDao){
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<Optional<RecordToEntity>> store(String recordId, String instanceId, String tenantId) {
    RecordToEntity recordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId).build();
    LOGGER.info("Saving RecordToInstance relationship: {}", recordToInstance.toString());
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToInstance, tenantId)
      .map(Optional.of(recordToInstance));
  }
}
