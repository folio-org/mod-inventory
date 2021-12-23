package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.util.EntityIdStorageDaoUtil;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityIdStorageDaoImpl implements EntityIdStorageDao {

  private final PostgresClientFactory postgresClientFactory;

  public EntityIdStorageDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Optional<RecordToEntity>> saveRecordToEntityRelationship(RecordToEntity recordToEntity, String tenantId) {
    return EntityIdStorageDaoUtil.save(postgresClientFactory, recordToEntity, tenantId);
  }

}
