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
import org.folio.inventory.services.AuthorityIdStorageService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthorityIdStorageServiceTest {
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String AUTHORITY_ID = UUID.randomUUID().toString();

  @Mock
  private EntityIdStorageDaoImpl entityIdStorageDaoImpl;
  @InjectMocks
  private AuthorityIdStorageService authorityIdStorageService;

  @Test
  public void shouldReturnSavedRecordToEntity() {
    RecordToEntity expectedRecordToAuthority = RecordToEntity.builder().table(EntityTable.AUTHORITY).recordId(RECORD_ID).entityId(AUTHORITY_ID).build();
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.succeededFuture(expectedRecordToAuthority));
    Future<RecordToEntity> future = authorityIdStorageService.store(RECORD_ID, AUTHORITY_ID, TENANT_ID);

    RecordToEntity actualRecordToAuthority = future.result();
    assertEquals(expectedRecordToAuthority.getTable().getTableName(), actualRecordToAuthority.getTable().getTableName());
    assertEquals(expectedRecordToAuthority.getTable().getEntityIdFieldName(), actualRecordToAuthority.getTable().getEntityIdFieldName());
    assertEquals(expectedRecordToAuthority.getTable().getRecordIdFieldName(), actualRecordToAuthority.getTable().getRecordIdFieldName());
    assertEquals(expectedRecordToAuthority.getRecordId(), actualRecordToAuthority.getRecordId());
    assertEquals(expectedRecordToAuthority.getEntityId(), actualRecordToAuthority.getEntityId());
  }

  @Test
  public void shouldReturnFailedFuture() {
    when(entityIdStorageDaoImpl.saveRecordToEntityRelationship(any(RecordToEntity.class), any())).thenReturn(Future.failedFuture("failed"));
    Future<RecordToEntity> future = authorityIdStorageService.store(RECORD_ID, AUTHORITY_ID, TENANT_ID);

    assertEquals("failed", future.cause().getMessage());
  }

}
