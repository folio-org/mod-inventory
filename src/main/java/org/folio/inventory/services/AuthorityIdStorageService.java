package org.folio.inventory.services;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;

public class AuthorityIdStorageService implements IdStorageService {
  private static final Logger LOGGER = LogManager.getLogger(AuthorityIdStorageService.class);

  private final EntityIdStorageDao entityIdStorageDao;

  public AuthorityIdStorageService(EntityIdStorageDao entityIdStorageDao) {
    this.entityIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<RecordToEntity> store(String recordId, String authorityId, String tenantId) {
    RecordToEntity recordToAuthority = RecordToEntity.builder().table(EntityTable.AUTHORITY).recordId(recordId).entityId(authorityId).build();
    LOGGER.info("Saving RecordToAuthority relationship: {}", recordToAuthority);
    return entityIdStorageDao.saveRecordToEntityRelationship(recordToAuthority, tenantId);
  }
}
