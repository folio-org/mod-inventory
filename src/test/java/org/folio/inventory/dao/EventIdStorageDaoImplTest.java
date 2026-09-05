package org.folio.inventory.dao;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.UUID;
import org.folio.inventory.common.dao.EventIdStorageDao;
import org.folio.inventory.common.dao.EventIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.domain.relationship.EventTable;
import org.folio.inventory.domain.relationship.EventToEntity;
import org.folio.inventory.resources.TenantApi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import support.PgPoolContainer;

@ExtendWith(VertxExtension.class)
class EventIdStorageDaoImplTest {

  private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
  private static final String EVENT_ID = UUID.randomUUID().toString();
  private static boolean runningOnOwn;

  private final PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  private final EventIdStorageDao eventIdStorageDao = new EventIdStorageDaoImpl(postgresClientFactory);

  @BeforeAll
  static void setUp() {
    if (!PgPoolContainer.isRunning()) {
      runningOnOwn = true;
      PgPoolContainer.create();
      TenantApi tenantApi = new TenantApi();
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
  void before() {
    postgresClientFactory.setShouldResetPool(true);
    PgPoolContainer.setEmbeddedPostgresOptions();
  }

  @Test
  void shouldReturnSavedEventId(VertxTestContext testContext) {
    var eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();
    var future = eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID);

    future.onComplete(testContext.succeeding(savedEventId -> testContext.verify(() -> {
      assertEquals(eventToEntity.getEventId(), savedEventId);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFuture(VertxTestContext testContext) {
    var eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
    var future = eventIdStorageDao.storeEvent(eventToEntity, TENANT_ID);

    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(ConnectException.class, err);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnSameInstanceIdWithDuplicateRecordId(VertxTestContext testContext) {
    var secondEventId = UUID.randomUUID().toString();

    var eventToEntity1 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(secondEventId).build();
    var eventToEntity2 = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(secondEventId).build();

    var future = eventIdStorageDao.storeEvent(eventToEntity1, TENANT_ID)
      .compose(ar -> eventIdStorageDao.storeEvent(eventToEntity2, TENANT_ID));

    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertTrue(err.getMessage().contains(UNIQUE_VIOLATION_SQL_STATE));
      testContext.completeNow();
    })));
  }
}
