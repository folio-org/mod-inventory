package org.folio.inventory.dao;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_HOST;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PORT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_DATABASE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_USERNAME;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PASSWORD;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_MAXPOOLSIZE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_SERVER_PEM;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_QUERYTIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class PostgresClientFactoryTest {

  private static final String TENANT_ID = "test_tenant";

  static Vertx vertx;

  @BeforeClass
  public static void setUp() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

  @Test
  public void shouldCreateCachedPool() {
    PgConnectOptions pgConnectOpts = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx, pgConnectOpts);
    PgPool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);

    assertNotNull(cachedPool);

    postgresClientFactory.close();
  }

  @Test
  public void shouldReturnPgPoolFromCache() {
    PgConnectOptions pgConnectOpts = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx, pgConnectOpts);
    PgPool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    PgPool poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertEquals(cachedPool, poolFromCache);

    postgresClientFactory.close();
  }

  @Test
  public void shouldResetPgPoolCache() {
    PgConnectOptions pgConnectOpts = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx, pgConnectOpts);
    PgPool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    postgresClientFactory.setShouldResetPool(true);
    PgPool poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertNotEquals(cachedPool, poolFromCache);

    postgresClientFactory.close();
  }

  @Test
  public void shouldSetDefaultConnectionOptions() {
    PgConnectOptions expectedPgConnectOptions = new PgConnectOptions();

    PostgresConnectionOptions.setConnectionOptions(new HashMap<>());
    PgConnectOptions actualConnectionOptions = PostgresConnectionOptions.getConnectionOptions(null);

    assertEquals(expectedPgConnectOptions.getHost(), actualConnectionOptions.getHost());
    assertEquals(expectedPgConnectOptions.getUser(), actualConnectionOptions.getUser());
    assertEquals(expectedPgConnectOptions.getPort(), actualConnectionOptions.getPort());
    assertEquals(expectedPgConnectOptions.getPassword(), actualConnectionOptions.getPassword());
    assertEquals(expectedPgConnectOptions.getDatabase(), actualConnectionOptions.getDatabase());
  }

  @Test
  public void shouldReturnInitializedConnectionOptions() {
    String serverPem = randomAlphaString(100);
    Set<String> expectedEnabledSecureTransportProtocols = Collections.singleton("TLSv1.3");
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put(DB_HOST, "localhost");
    propertiesMap.put(DB_PORT, "5432");
    propertiesMap.put(DB_USERNAME, "test");
    propertiesMap.put(DB_PASSWORD, "test");
    propertiesMap.put(DB_DATABASE, "test");
    propertiesMap.put(DB_MAXPOOLSIZE, String.valueOf(5));
    propertiesMap.put(DB_SERVER_PEM, serverPem);
    propertiesMap.put(DB_QUERYTIMEOUT, String.valueOf(60000));

    PostgresConnectionOptions.setConnectionOptions(propertiesMap);
    PgConnectOptions pgConnectOpts = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);

    assertEquals("localhost", pgConnectOpts.getHost());
    assertEquals(5432, pgConnectOpts.getPort());
    assertEquals("test", pgConnectOpts.getUser());
    assertEquals("test", pgConnectOpts.getPassword());
    assertEquals("test", pgConnectOpts.getDatabase());
    assertEquals(60000, pgConnectOpts.getIdleTimeout());
    assertEquals(SslMode.VERIFY_FULL, pgConnectOpts.getSslMode());
    assertEquals("HTTPS", pgConnectOpts.getHostnameVerificationAlgorithm());
    assertNotNull(pgConnectOpts.getPemTrustOptions());
    assertEquals(expectedEnabledSecureTransportProtocols, pgConnectOpts.getEnabledSecureTransportProtocols());
    assertNotNull(pgConnectOpts.getOpenSslEngineOptions());

    PostgresConnectionOptions.setConnectionOptions(new HashMap<>());
  }

  public static String randomAlphaString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      char c = (char) (65 + 25 * Math.random());
      builder.append(c);
    }
    return builder.toString();
  }
}
