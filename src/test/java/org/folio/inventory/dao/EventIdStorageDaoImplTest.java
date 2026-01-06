package org.folio.inventory.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.common.dao.EventIdStorageDao;
import org.folio.inventory.common.dao.EventIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.domain.relationship.EventTable;
import org.folio.inventory.domain.relationship.EventToEntity;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.rest.impl.PgPoolContainer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;

@RunWith(VertxUnitRunner.class)
public class EventIdStorageDaoImplTest {

  private static final String EVENT_ID = UUID.randomUUID().toString();
  private static boolean runningOnOwn;

  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  private EventIdStorageDao eventIdStorageDao = new EventIdStorageDaoImpl(postgresClientFactory);

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

//  @Test
//  TODO: "Future{cause=Scram authentication not supported, missing com.ongres.scram:scram-client on the class/module path}"
//  public void shouldReturnSavedEventId(TestContext context) {
//    Async async = context.async();
//
//    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();
//    Future<String> future = eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID);
//
//    future.onComplete(ar -> {
//      context.assertTrue(ar.succeeded());
//      String savedEventId = ar.result();
//      context.assertEquals(eventToEntity.getEventId(), savedEventId);
//      async.complete();
//    });
//  }

  @Test
  public void shouldReturnFailedFuture(TestContext context) {
    Async async = context.async();

    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
    Future<String> future = eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

//  @Test
//  TODO: "Future{cause=Scram authentication not supported, missing com.ongres.scram:scram-client on the class/module path}"
//  public void shouldReturnSameInstanceIdWithDuplicateRecordId(TestContext context) {
//    Async async = context.async();
//
//    String SECOND_EVENT_ID = UUID.randomUUID().toString();
//
//    EventToEntity eventToEntity1 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(SECOND_EVENT_ID).build();
//    EventToEntity eventToEntity2 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(SECOND_EVENT_ID).build();
//
//    Future<String> future = eventIdStorageDao.storeEvent(eventToEntity1, TENANT_ID)
//      .compose(ar -> eventIdStorageDao.storeEvent(eventToEntity2, TENANT_ID));
//
//    future.onComplete(ar -> {
//      context.assertTrue(ar.failed());
//      context.assertTrue(ar.cause().getMessage().contains(UNIQUE_VIOLATION_SQL_STATE));
//      async.complete();
//    });
//  }
}
