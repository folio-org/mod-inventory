package org.folio.inventory.dao;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.pgclient.SslMode;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.rest.impl.PgPoolContainer;
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
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_IDLETIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class PostgresClientFactoryTest {

  private static final String TENANT_ID = "test_tenant";
  private static final Integer MAX_POOL_SIZE = 5;
  private static final String SERVER_PEM = randomAlphaString(100);

  static Vertx vertx;

  @BeforeClass
  public static void setUp() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close()
      .onComplete(context.asyncAssertSuccess(res -> {
        async.complete();
      }));
    PgPoolContainer.setEmbeddedPostgresOptions();
    PostgresClientFactory.closeAll();
  }

  @Test
  public void shouldCreateCachedPool() {
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx);
    Pool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);

    assertNotNull(cachedPool);
  }

  @Test
  public void shouldReturnPgPoolFromCache() {
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx);
    Pool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    Pool poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertEquals(cachedPool, poolFromCache);
  }

  @Test
  public void shouldResetPgPoolCache() {
    PostgresClientFactory postgresClientFactory =
      new PostgresClientFactory(vertx);
    Pool cachedPool = postgresClientFactory.getCachedPool(TENANT_ID);
    postgresClientFactory.setShouldResetPool(true);
    Pool poolFromCache = postgresClientFactory.getCachedPool(TENANT_ID);
    assertNotNull(cachedPool);
    assertNotNull(poolFromCache);
    assertNotEquals(cachedPool, poolFromCache);
  }

  @Test
  public void shouldSetDefaultConnectionOptions() {
    PgConnectOptions expectedPgConnectOptions = new PgConnectOptions();
    PostgresConnectionOptions.setSystemProperties(new HashMap<>());

    PgConnectOptions actualConnectionOptions = PostgresConnectionOptions.getConnectionOptions(null);

    assertEquals(expectedPgConnectOptions.getHost(), actualConnectionOptions.getHost());
    assertEquals(expectedPgConnectOptions.getUser(), actualConnectionOptions.getUser());
    assertEquals(expectedPgConnectOptions.getPort(), actualConnectionOptions.getPort());
    assertEquals(expectedPgConnectOptions.getPassword(), actualConnectionOptions.getPassword());
    assertEquals(expectedPgConnectOptions.getDatabase(), actualConnectionOptions.getDatabase());
  }

  @Test
  public void shouldReturnInitializedConnectionOptions() {
    Set<String> expectedEnabledSecureTransportProtocols = Collections.singleton("TLSv1.3");
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put(DB_HOST, "localhost");
    optionsMap.put(DB_PORT, "5432");
    optionsMap.put(DB_USERNAME, "test");
    optionsMap.put(DB_PASSWORD, "test");
    optionsMap.put(DB_DATABASE, "test");
    optionsMap.put(DB_MAXPOOLSIZE, String.valueOf(MAX_POOL_SIZE));
    optionsMap.put(DB_SERVER_PEM, SERVER_PEM);
    optionsMap.put(DB_IDLETIMEOUT, String.valueOf(60000));

    PostgresConnectionOptions.setSystemProperties(optionsMap);
    PgConnectOptions pgConnectOpts = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);

    assertEquals("localhost", pgConnectOpts.getHost());
    assertEquals(5432, pgConnectOpts.getPort());
    assertEquals("test", pgConnectOpts.getUser());
    assertEquals("test", pgConnectOpts.getPassword());
    assertEquals("test", pgConnectOpts.getDatabase());
    //assertEquals(60000, pgConnectOpts.getIdleTimeout());
    assertEquals(SslMode.VERIFY_FULL, pgConnectOpts.getSslMode());
    assertEquals("HTTPS", pgConnectOpts.getSslOptions().getHostnameVerificationAlgorithm());
    assertEquals(MAX_POOL_SIZE, PostgresConnectionOptions.getMaxPoolSize());
    assertNotNull(pgConnectOpts.getSslOptions().getTrustOptions());
    assertEquals(expectedEnabledSecureTransportProtocols, pgConnectOpts.getSslOptions().getEnabledSecureTransportProtocols());

    PostgresConnectionOptions.setSystemProperties(new HashMap<>());
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
