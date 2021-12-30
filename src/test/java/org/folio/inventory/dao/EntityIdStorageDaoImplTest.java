package org.folio.inventory.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
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
  public void shouldReturnSavedRecordToEntity(TestContext context) {
    Async async = context.async();

    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();

    Future<RecordToEntity> future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      RecordToEntity actualRecordToEntity = ar.result();
      context.assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToEntity.getRecordId());
      context.assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToEntity.getEntityId());
      context.assertEquals(expectedRecordToInstance.getTable(), actualRecordToEntity.getTable());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFuture(TestContext context) {
    Async async = context.async();

    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
    Future<RecordToEntity> future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnSameInstanceIdWithDuplicateRecordId(TestContext context) {
    Async async = context.async();

    RecordToEntity expectedRecordToInstance1 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(INSTANCE_ID).build();
    RecordToEntity expectedRecordToInstance2 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(RECORD_ID).entityId(DUPLICATE_INSTANCE_ID).build();

    Future<RecordToEntity> future = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance1, TENANT_ID)
      .compose(ar -> entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance2, TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      RecordToEntity actualRecordToEntity = ar.result();
      context.assertEquals(expectedRecordToInstance1.getRecordId(), actualRecordToEntity.getRecordId());
      context.assertEquals(expectedRecordToInstance1.getEntityId(), actualRecordToEntity.getEntityId());
      context.assertEquals(expectedRecordToInstance1.getTable(), actualRecordToEntity.getTable());
      async.complete();
    });
  }
}
