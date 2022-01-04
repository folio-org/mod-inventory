package org.folio.inventory.service;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.UUID;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.HoldingsIdStorageService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HoldingsIdStorageServiceTest {
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String HOLDING_ID = UUID.randomUUID().toString();

  @Mock
  private EntityIdStorageDaoImpl entityIdStorageDaoImpl;
  @InjectMocks
  private HoldingsIdStorageService holdingsIdStorageService;

  @Test
  public void shouldReturnSavedRecordToEntity() {
    RecordToEntity expectedRecordToHoldings = RecordToEntity.builder().table(EntityTable.HOLDINGS).recordId(RECORD_ID).entityId(HOLDING_ID).build();
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.succeededFuture(expectedRecordToHoldings));
    Future<RecordToEntity> future = holdingsIdStorageService.store(RECORD_ID, HOLDING_ID, TENANT_ID);

    RecordToEntity actualRecordToHoldings = future.result();
    assertEquals(expectedRecordToHoldings.getTable().getTableName(), actualRecordToHoldings.getTable().getTableName());
    assertEquals(expectedRecordToHoldings.getTable().getEntityIdFieldName(), actualRecordToHoldings.getTable().getEntityIdFieldName());
    assertEquals(expectedRecordToHoldings.getTable().getRecordIdFieldName(), actualRecordToHoldings.getTable().getRecordIdFieldName());
    assertEquals(expectedRecordToHoldings.getRecordId(), actualRecordToHoldings.getRecordId());
    assertEquals(expectedRecordToHoldings.getEntityId(), actualRecordToHoldings.getEntityId());
  }

  @Test
  public void shouldReturnFailedFuture() {
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.failedFuture("failed"));
    Future<RecordToEntity> future = holdingsIdStorageService.store(RECORD_ID, HOLDING_ID, TENANT_ID);

    assertEquals("failed", future.cause().getMessage());
  }

}
