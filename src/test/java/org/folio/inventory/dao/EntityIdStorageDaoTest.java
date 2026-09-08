package org.folio.inventory.dao;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.UUID;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.resources.TenantApi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import support.PgPoolContainer;

@ExtendWith(VertxExtension.class)
class EntityIdStorageDaoTest {

  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String DUPLICATE_INSTANCE_ID = UUID.randomUUID().toString();

  private static boolean runningOnOwn;

  private final Vertx vertx = Vertx.vertx();
  private final PostgresClientFactory postgresClientFactory =
    new PostgresClientFactory(vertx, new PostgresConnectionOptions(PgPoolContainer.getConnectionEnv()));
  private final EntityIdStorageDao entityIdStorageDao = new EntityIdStorageDao(postgresClientFactory);

  @BeforeAll
  static void setUp() {
    if (!PgPoolContainer.isRunning()) {
      runningOnOwn = true;
      PgPoolContainer.create();
      TenantApi tenantApi = new TenantApi(new PostgresConnectionOptions(PgPoolContainer.getConnectionEnv()));
      tenantApi.initializeSchemaForTenant(TENANT_ID);
    }
  }

  @AfterAll
  static void tearDown() {
    if (PgPoolContainer.isRunning() && runningOnOwn) {
      PgPoolContainer.stop();
    }
  }

  @BeforeEach
  void before() throws Exception {
    PostgresClientFactory.closePool(TENANT_ID).toCompletionStage().toCompletableFuture().get();
  }

  @Test
  void shouldReturnSavedRecordToInstance(VertxTestContext testContext) {
    var expectedRecordToInstance = RecordToEntity.builder()
      .table(EntityTable.INSTANCE)
      .recordId(RECORD_ID)
      .entityId(INSTANCE_ID)
      .build();

    var future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);

    future.onComplete(testContext.succeeding(actualRecordToEntity -> testContext.verify(() -> {
      assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToEntity.getRecordId());
      assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToEntity.getEntityId());
      assertEquals(expectedRecordToInstance.getTable(), actualRecordToEntity.getTable());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnSavedRecordToItem(VertxTestContext testContext) {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String itemId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    var expectedRecordToItem = RecordToEntity.builder()
      .table(EntityTable.ITEM)
      .recordId(recordId)
      .entityId(itemId)
      .build();

    var optionalFuture = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToItem, TENANT_ID);
    optionalFuture.onComplete(testContext.succeeding(actualRecordToEntity -> testContext.verify(() -> {
      assertEquals(expectedRecordToItem.getRecordId(), actualRecordToEntity.getRecordId());
      assertEquals(expectedRecordToItem.getEntityId(), actualRecordToEntity.getEntityId());
      assertEquals(expectedRecordToItem.getTable(), actualRecordToEntity.getTable());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFuture_whenNoDatabaseParamsSpecified(VertxTestContext testContext) {
    var expectedRecordToInstance = RecordToEntity.builder()
      .table(EntityTable.INSTANCE)
      .recordId(RECORD_ID)
      .entityId(INSTANCE_ID)
      .build();
    var daoWithoutDbParams = new EntityIdStorageDao(
      new PostgresClientFactory(vertx, new PostgresConnectionOptions(new HashMap<>())));

    var future = daoWithoutDbParams.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);

    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(ConnectException.class, err);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnSameInstanceIdWithDuplicateRecordId(VertxTestContext testContext) {
    var expectedRecordToInstance1 = RecordToEntity.builder()
      .table(EntityTable.INSTANCE)
      .recordId(RECORD_ID)
      .entityId(INSTANCE_ID)
      .build();
    var expectedRecordToInstance2 = RecordToEntity.builder()
      .table(EntityTable.INSTANCE)
      .recordId(RECORD_ID)
      .entityId(DUPLICATE_INSTANCE_ID)
      .build();

    var future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance1, TENANT_ID)
      .compose(ar -> entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance2, TENANT_ID));

    future.onComplete(testContext.succeeding(actualRecordToEntity -> testContext.verify(() -> {
      assertEquals(expectedRecordToInstance1.getRecordId(), actualRecordToEntity.getRecordId());
      assertEquals(expectedRecordToInstance1.getEntityId(), actualRecordToEntity.getEntityId());
      assertEquals(expectedRecordToInstance1.getTable(), actualRecordToEntity.getTable());
      testContext.completeNow();
    })));
  }
}
