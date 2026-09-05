package org.folio.inventory.service;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.ItemIdStorageService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ItemIdStorageServiceTest {

  @Mock
  private EntityIdStorageDaoImpl entityIdStorageDaoImpl;
  @InjectMocks
  private ItemIdStorageService itemIdStorageService;

  @Test
  void shouldReturnSavedRecordToEntity() {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String itemId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    RecordToEntity expectedRecordToItem = RecordToEntity.builder()
      .table(EntityTable.ITEM).recordId(recordId).entityId(itemId).build();
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any()))
      .thenReturn(Future.succeededFuture(expectedRecordToItem));
    Future<RecordToEntity> future = itemIdStorageService.store(recordId, itemId, TENANT_ID);

    RecordToEntity actualRecordToItem = future.result();
    assertEquals(expectedRecordToItem.getTable().getTableName(), actualRecordToItem.getTable().getTableName());
    assertEquals(expectedRecordToItem.getTable().getEntityIdFieldName(),
      actualRecordToItem.getTable().getEntityIdFieldName());
    assertEquals(expectedRecordToItem.getTable().getRecordIdFieldName(),
      actualRecordToItem.getTable().getRecordIdFieldName());
    assertEquals(expectedRecordToItem.getRecordId(), actualRecordToItem.getRecordId());
    assertEquals(expectedRecordToItem.getEntityId(), actualRecordToItem.getEntityId());
  }

  @Test
  void shouldReturnFailedFuture() {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String itemId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any()))
      .thenReturn(Future.failedFuture("failed"));
    Future<RecordToEntity> future = itemIdStorageService.store(recordId, itemId, TENANT_ID);

    assertEquals("failed", future.cause().getMessage());
  }
}
