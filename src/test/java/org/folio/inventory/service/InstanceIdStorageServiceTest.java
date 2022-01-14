package org.folio.inventory.service;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.InstanceIdStorageService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InstanceIdStorageServiceTest {
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String INSTANCE_ID = UUID.randomUUID().toString();

  @Mock
  private EntityIdStorageDaoImpl entityIdStorageDaoImpl;
  @InjectMocks
  private InstanceIdStorageService instanceIdStorageService;

  @Test
  public void shouldReturnSavedRecordToEntity() {
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.succeededFuture(expectedRecordToInstance));
    Future<RecordToEntity> future = instanceIdStorageService.store(RECORD_ID, INSTANCE_ID, TENANT_ID);

    RecordToEntity actualRecordToInstance = future.result();
    assertEquals(expectedRecordToInstance.getTable().getTableName(), actualRecordToInstance.getTable().getTableName());
    assertEquals(expectedRecordToInstance.getTable().getEntityIdFieldName(), actualRecordToInstance.getTable().getEntityIdFieldName());
    assertEquals(expectedRecordToInstance.getTable().getRecordIdFieldName(), actualRecordToInstance.getTable().getRecordIdFieldName());
    assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToInstance.getRecordId());
    assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToInstance.getEntityId());
  }

  @Test
  public void shouldReturnFailedFuture() {
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.failedFuture("failed"));
    Future<RecordToEntity> future = instanceIdStorageService.store(RECORD_ID, INSTANCE_ID, TENANT_ID);

    assertEquals("failed", future.cause().getMessage());
  }

}
