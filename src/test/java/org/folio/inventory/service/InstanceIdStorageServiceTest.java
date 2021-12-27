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

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InstanceIdStorageServiceTest {

  private static final String TENANT_ID = "test_tenant";

  @Mock
  private EntityIdStorageDaoImpl entityIdStorageDaoImpl;
  @InjectMocks
  private InstanceIdStorageService instanceIdStorageService;

  @Test
  public void shouldReturnSavedRecordToEntity() {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId).build();
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.succeededFuture(Optional.of(expectedRecordToInstance)));
    Future<Optional<RecordToEntity>> optionalFuture = instanceIdStorageService.store(recordId, instanceId, TENANT_ID);

    Optional<RecordToEntity> optionalRecordToEntity = optionalFuture.result();
    assertTrue(optionalRecordToEntity.isPresent());

    RecordToEntity actualRecordToInstance = optionalRecordToEntity.get();
    assertEquals(expectedRecordToInstance.getTable().getTableName(), actualRecordToInstance.getTable().getTableName());
    assertEquals(expectedRecordToInstance.getTable().getEntityIdFieldName(), actualRecordToInstance.getTable().getEntityIdFieldName());
    assertEquals(expectedRecordToInstance.getTable().getRecordIdFieldName(), actualRecordToInstance.getTable().getRecordIdFieldName());
    assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToInstance.getRecordId());
    assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToInstance.getEntityId());
  }

  @Test
  public void shouldReturnRFailedFuture() {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.failedFuture("failed"));
    Future<Optional<RecordToEntity>> optionalFuture = instanceIdStorageService.store(recordId, instanceId, TENANT_ID);

    assertEquals("failed", optionalFuture.cause().getMessage());
  }

}
