package org.folio.inventory.dao;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_DATABASE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_HOST;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_IDLETIMEOUT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_MAXPOOLSIZE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PASSWORD;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PORT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_SERVER_PEM;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class PostgresClientFactoryTest {

  private static final String TENANT_ID = "test_tenant";
  private static final Integer MAX_POOL_SIZE = 5;
  private static final String SERVER_PEM = "a".repeat(100);

  @Test
  void shouldCreateCachedPool(Vertx vertx) {
    var postgresClientFactory = new PostgresClientFactory(vertx);
    var cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);

    assertNotNull(cachedPool);
  }

  @Test
  void shouldReturnPgPoolFromCache(Vertx vertx) {
    var postgresClientFactory = new PostgresClientFactory(vertx);
    var cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    var poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertEquals(cachedPool, poolFromCache);
  }

  @DisplayName("should create a new pool after the cached pool is closed")
  @Test
  void shouldReturnNewPool_whenCachedPoolClosed(Vertx vertx, VertxTestContext testContext) {
    // arrange
    var postgresClientFactory = new PostgresClientFactory(vertx);
    var cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);

    // act
    PostgresClientFactory.closePool(TENANT_ID).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      var poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);

      // assert
      assertNotNull(poolFromCache);
      assertNotEquals(cachedPool, poolFromCache);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldSetDefaultConnectionOptions() {
    var expectedPgConnectOptions = new PgConnectOptions();

    var actualConnectionOptions = new PostgresConnectionOptions(new HashMap<>()).getConnectionOptions(null);

    assertEquals(expectedPgConnectOptions.getHost(), actualConnectionOptions.getHost());
    assertEquals(expectedPgConnectOptions.getUser(), actualConnectionOptions.getUser());
    assertEquals(expectedPgConnectOptions.getPort(), actualConnectionOptions.getPort());
    assertEquals(expectedPgConnectOptions.getPassword(), actualConnectionOptions.getPassword());
    assertEquals(expectedPgConnectOptions.getDatabase(), actualConnectionOptions.getDatabase());
  }

  @Test
  void shouldReturnInitializedConnectionOptions() {
    var expectedEnabledSecureTransportProtocols = Collections.singleton("TLSv1.3");
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put(DB_HOST, "localhost");
    optionsMap.put(DB_PORT, "5432");
    optionsMap.put(DB_USERNAME, "test");
    optionsMap.put(DB_PASSWORD, "test");
    optionsMap.put(DB_DATABASE, "test");
    optionsMap.put(DB_MAXPOOLSIZE, String.valueOf(MAX_POOL_SIZE));
    optionsMap.put(DB_SERVER_PEM, SERVER_PEM);
    optionsMap.put(DB_IDLETIMEOUT, String.valueOf(60000));

    var connectionOptions = new PostgresConnectionOptions(optionsMap);
    var pgConnectOpts = connectionOptions.getConnectionOptions(TENANT_ID);

    assertEquals("localhost", pgConnectOpts.getHost());
    assertEquals(5432, pgConnectOpts.getPort());
    assertEquals("test", pgConnectOpts.getUser());
    assertEquals("test", pgConnectOpts.getPassword());
    assertEquals("test", pgConnectOpts.getDatabase());
    assertEquals(SslMode.VERIFY_FULL, pgConnectOpts.getSslMode());
    assertEquals("HTTPS", pgConnectOpts.getSslOptions().getHostnameVerificationAlgorithm());
    assertEquals(MAX_POOL_SIZE, connectionOptions.getMaxPoolSize());
    assertNotNull(pgConnectOpts.getSslOptions().getTrustOptions());
    assertEquals(60000, connectionOptions.getPoolOptions().getIdleTimeout());
    assertEquals(expectedEnabledSecureTransportProtocols,
      pgConnectOpts.getSslOptions().getEnabledSecureTransportProtocols());
  }
}
