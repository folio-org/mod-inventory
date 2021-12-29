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
import org.folio.inventory.resources.SchemaApi;
import org.folio.inventory.rest.impl.PgPoolContainer;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;

import static api.ApiTestSuite.TENANT_ID;

@RunWith(VertxUnitRunner.class)
public class EntityIdStorageDaoImplTest {
  private static boolean runningOnOwn;

  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  private EntityIdStorageDao entityIdStorageDao = new EntityIdStorageDaoImpl(postgresClientFactory);

  @BeforeClass
  public static void setUp() {
    if (!PgPoolContainer.isRunning()) {
      System.out.println("Running test on own, creating PostgreSQL container manually");
      runningOnOwn = true;
      PgPoolContainer.create();
      SchemaApi schemaApi = new SchemaApi();
      schemaApi.initializeSchemaForTenant(TENANT_ID);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (PgPoolContainer.isRunning() && runningOnOwn) {
      System.out.println("Running test on own, stopping PostgreSQL container manually");
      PgPoolContainer.stop();
    }
  }

  @Test
  public void shouldReturnSavedRecordToEntity(TestContext context) {
    Async async = context.async();

    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId).build();
    Future<Optional<RecordToEntity>> optionalFuture = Future.succeededFuture().compose(c -> entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID));

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      if (ar.result().isPresent()) {
        System.out.println("3");
        RecordToEntity actualRecordToEntity = ar.result().get();
        System.out.println(actualRecordToEntity);
        context.assertEquals(expectedRecordToInstance.getRecordId(), actualRecordToEntity.getRecordId());
        context.assertEquals(expectedRecordToInstance.getEntityId(), actualRecordToEntity.getEntityId());
        context.assertEquals(expectedRecordToInstance.getTable(), actualRecordToEntity.getTable());
        System.out.println("3333");
      }
      System.out.println("33");
      async.complete();
      System.out.println("333");
    });
  }

  @Test
  @Ignore
  public void shouldReturnFailedFuture(TestContext context) {
    Async async = context.async();

    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    RecordToEntity expectedRecordToInstance = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId).build();

    Future<Optional<RecordToEntity>> optionalFuture = Future.succeededFuture().compose(c -> {
      PostgresConnectionOptions.setSystemProperties(new HashMap<>());
      return entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance, TENANT_ID);
    });

    optionalFuture.onComplete(ar -> {
      System.out.println("2");
      System.out.println(ar);
      context.assertTrue(ar.failed());
      System.out.println("22");
      //"Connection refused: localhost/127.0.0.1:5432"
      async.complete();
      System.out.println("222");
    });
    PgPoolContainer.setEmbeddedPostgresOptions();
  }

  @Test
  public void shouldReturnSameInstanceIdWithDuplicateRecordId(TestContext context) {
    Async async = context.async();

    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId1 = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String instanceId2 = "25b136b2-341a-4453-bf24-41a0cf15b5f4";
    RecordToEntity expectedRecordToInstance1 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId1).build();
    RecordToEntity expectedRecordToInstance2 = RecordToEntity.builder().table(EntityTable.INSTANCE).recordId(recordId).entityId(instanceId2).build();

    Future<Optional<RecordToEntity>> optionalFuture1 = entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance1, TENANT_ID)
      .compose(ar -> entityIdStorageDao.saveRecordToEntityRelationship(expectedRecordToInstance2, TENANT_ID));

    optionalFuture1.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      if (ar.result().isPresent()) {
        System.out.println("1");
        RecordToEntity actualRecordToEntity = ar.result().get();
        System.out.println(actualRecordToEntity);
        context.assertEquals(expectedRecordToInstance1.getRecordId(), actualRecordToEntity.getRecordId());
        context.assertEquals(expectedRecordToInstance1.getEntityId(), actualRecordToEntity.getEntityId());
        context.assertEquals(expectedRecordToInstance1.getTable(), actualRecordToEntity.getTable());
      }
      System.out.println("11");
      async.complete();
      System.out.println("111");
    });
  }
}
