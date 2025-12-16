package org.folio.inventory.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.rest.impl.PgPoolContainer;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;

@RunWith(VertxUnitRunner.class)
public class EntityIdStorageDaoImplTest {
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String DUPLICATE_INSTANCE_ID = UUID.randomUUID().toString();

  private static boolean runningOnOwn;

  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  private EntityIdStorageDao entityIdStorageDao = new EntityIdStorageDaoImpl(postgresClientFactory);

  @BeforeClass
  public static void setUp() {
    if (!PgPoolContainer.isRunning()) {
      runningOnOwn = true;
      PgPoolContainer.create();
      TenantApi tenantApi = new TenantApi();
      tenantApi.initializeSchemaForTenant(TENANT_ID);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (PgPoolContainer.isRunning() && runningOnOwn) {
      PgPoolContainer.stop();
    }
  }

  @Before
  public void before() {
    postgresClientFactory.setShouldResetPool(true);
    PgPoolContainer.setEmbeddedPostgresOptions();
  }

  @Test
  public void shouldReturnSavedRecordToInstance(TestContext context) {
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();

    Future<RecordToEntity> future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);

    future.onComplete(context.asyncAssertSuccess(actualRecordToEntity -> {
      context.assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToEntity.getRecordId());
      context.assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToEntity.getEntityId());
      context.assertEquals(expectedRecordToInstance.getTable(), actualRecordToEntity.getTable());
    }));
  }

  @Test
  public void shouldReturnSavedRecordToItem(TestContext context) {
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String itemId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    RecordToEntity expectedRecordToItem = RecordToEntity.builder().table(EntityTable.ITEM).recordId(recordId).entityId(itemId).build();

    Future<RecordToEntity> optionalFuture = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToItem, TENANT_ID);
    optionalFuture.onComplete(context.asyncAssertSuccess(actualRecordToEntity -> {
      context.assertEquals(expectedRecordToItem.getRecordId(), actualRecordToEntity.getRecordId());
      context.assertEquals(expectedRecordToItem.getEntityId(), actualRecordToEntity.getEntityId());
      context.assertEquals(expectedRecordToItem.getTable(), actualRecordToEntity.getTable());
    }));
  }

  @Test
  public void shouldReturnFailedFuture(TestContext context) {
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
    entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID)
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void shouldReturnSameInstanceIdWithDuplicateRecordId(TestContext context) {
    RecordToEntity expectedRecordToInstance1 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();
    RecordToEntity expectedRecordToInstance2 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(DUPLICATE_INSTANCE_ID).build();

    entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance1, TENANT_ID)
    .compose(ar -> entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance2, TENANT_ID))
    .onComplete(context.asyncAssertSuccess(actualRecordToEntity -> {
      context.assertEquals(expectedRecordToInstance1.getRecordId(), actualRecordToEntity.getRecordId());
      context.assertEquals(expectedRecordToInstance1.getEntityId(), actualRecordToEntity.getEntityId());
      context.assertEquals(expectedRecordToInstance1.getTable(), actualRecordToEntity.getTable());
    }));
  }
}
