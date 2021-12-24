package org.folio.inventory.dao;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.folio.inventory.common.dao.PostgresClientFactory.setDefaultConnectionOptions;
import static org.folio.inventory.common.dao.PostgresClientFactory.setProperties;
import static org.folio.inventory.common.dao.PostgresClientFactory.getProperties;
import static org.folio.inventory.common.dao.PostgresClientFactory.getConnectionOptions;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_HOST;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_PORT;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_DATABASE;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_USERNAME;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_PASSWORD;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_MAXPOOLSIZE;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_SERVER_PEM;
import static org.folio.inventory.common.dao.PostgresClientFactory.DB_QUERYTIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@RunWith(VertxUnitRunner.class)
public class PostgresClientFactoryTest {

  private static final String TENANT_ID = "test_tenant";

  static Vertx vertx;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

  @Test
  public void shouldCreateCachedPool() {
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    PgPool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);

    assertNotNull(cachedPool);

    postgresClientFactory.close();
  }

  @Test
  public void shouldReturnPgPoolFromCache() {
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    PgPool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    PgPool poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertEquals(cachedPool, poolFromCache);

    postgresClientFactory.close();
  }

  @Test
  public void shouldResetPgPoolCache() {
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
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
    setDefaultConnectionOptions(expectedPgConnectOptions);
    PgConnectOptions connectionOptions = getConnectionOptions(null);
    assertEquals(expectedPgConnectOptions, connectionOptions);

    setDefaultConnectionOptions(new PgConnectOptions());
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
    setProperties(propertiesMap);

    Map<String, String> properties = getProperties();
    PgConnectOptions connectionOptions = getConnectionOptions(TENANT_ID);

    assertEquals(8, properties.size());
    assertEquals("localhost", connectionOptions.getHost());
    assertEquals(5432, connectionOptions.getPort());
    assertEquals("test", connectionOptions.getUser());
    assertEquals("test", connectionOptions.getPassword());
    assertEquals("test", connectionOptions.getDatabase());
    assertEquals(60000, connectionOptions.getIdleTimeout());
    assertEquals(SslMode.VERIFY_FULL, connectionOptions.getSslMode());
    assertEquals("HTTPS", connectionOptions.getHostnameVerificationAlgorithm());
    assertNotNull(connectionOptions.getPemTrustOptions());
    assertEquals(expectedEnabledSecureTransportProtocols, connectionOptions.getEnabledSecureTransportProtocols());
    assertNotNull(connectionOptions.getOpenSslEngineOptions());

    setProperties(new HashMap<>());
    setDefaultConnectionOptions(new PgConnectOptions());
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
