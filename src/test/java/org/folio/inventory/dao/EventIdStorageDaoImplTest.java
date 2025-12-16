package org.folio.inventory.dao;

import io.vertx.core.Vertx;
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
  public static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
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

  @Test
  public void shouldReturnSavedEventId(TestContext context) {
    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();
    eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID)
    .onComplete(context.asyncAssertSuccess(savedEventId -> {
      context.assertEquals(eventToEntity.getEventId(), savedEventId);
    }));
  }

  @Test
  public void shouldReturnFailedFuture(TestContext context) {
    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
    eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID)
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void shouldReturnSameInstanceIdWithDuplicateRecordId(TestContext context) {
    String SECOND_EVENT_ID = UUID.randomUUID().toString();

    EventToEntity eventToEntity1 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(SECOND_EVENT_ID).build();
    EventToEntity eventToEntity2 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(SECOND_EVENT_ID).build();

    eventIdStorageDao.storeEvent(eventToEntity1, TENANT_ID)
    .compose(ar -> eventIdStorageDao.storeEvent(eventToEntity2, TENANT_ID))
    .onComplete(context.asyncAssertFailure(e -> {
      context.assertTrue(e.getMessage().contains(UNIQUE_VIOLATION_SQL_STATE));
    }));
  }
}
