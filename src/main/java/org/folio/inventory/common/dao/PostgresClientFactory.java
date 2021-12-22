package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.folio.inventory.rest.util.ModuleUtil.convertToPsqlStandard;
import static org.folio.inventory.rest.util.ModuleName.getModuleName;

@Component
public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger(PostgresClientFactory.class);

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();
  private static final String MODULE_NAME = getModuleName();
  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";
  private static final Integer DEFAULT_IDLE_TIMEOUT = 60000;

  static String host = System.getenv("DB_HOST");
  static String port = System.getenv("DB_PORT");
  static String user = System.getenv("DB_USERNAME");
  static String password = System.getenv("DB_PASSWORD");
  static String database = System.getenv("DB_DATABASE");
  static String maxPoolSize = System.getenv("DB_MAXPOOLSIZE");
  static String serverPem = System.getenv("DB_SERVER_PEM");
  static String queryTimeout = System.getenv("DB_QUERYTIMEOUT");

  static PgConnectOptions pgConnectOptions = new PgConnectOptions();

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   */
  private static boolean shouldResetPool = false;

  private Vertx vertx;

  @Autowired
  public PostgresClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  @PreDestroy
  public void close() {
    closeAll();
  }

  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
  }

  private static PgPool getCachedPool(Vertx vertx, String tenantId) {
    // assumes a single thread Vert.x model so no synchronized needed
    if (POOL_CACHE.containsKey(tenantId) && !shouldResetPool) {
      LOGGER.debug("Using existing database connection pool for tenant {}.", tenantId);
      return POOL_CACHE.get(tenantId);
    }
    if (shouldResetPool) {
      POOL_CACHE.remove(tenantId);
      shouldResetPool = false;
    }
    LOGGER.info("Creating new database connection pool for tenant {}.", tenantId);
    PgConnectOptions connectOptions = getConnectionOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(Integer.parseInt(maxPoolSize));
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, client);

    return client;
  }

  /**
   * Get {@link PgPool}
   *
   * @param tenantId tenant id
   * @return pooled database client
   */
  public PgPool getCachedPool(String tenantId) {
    return getCachedPool(this.vertx, tenantId);
  }

  public static void setDefaultConnectOptions(PgConnectOptions connectOptions) {
    PostgresClientFactory.pgConnectOptions = connectOptions;
  }

  /**
   * Get {@link PgConnectOptions}
   *
   * @param tenantId tenant id
   * @return postgres connection options
   */
  public static PgConnectOptions getConnectionOptions(String tenantId) {
    PgConnectOptions connectOptions = pgConnectOptions;

    fillPgConnectOptions(connectOptions);

    if (tenantId != null) {
      connectOptions.addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    }

    return connectOptions;
  }

  public static void fillPgConnectOptions(PgConnectOptions connectOptions) {

    connectOptions.getProperties().put("application_name", MODULE_NAME);

    if (host != null) {
      connectOptions.setHost(host);
    }
    if (port != null) {
      connectOptions.setPort(Integer.parseInt(port));
    }
    if (database != null) {
      connectOptions.setDatabase(database);
    }
    if (user != null) {
      connectOptions.setUser(user);
    }
    if (password != null) {
      connectOptions.setPassword(password);
    }
    if (serverPem != null) {
      connectOptions.setSslMode(SslMode.VERIFY_FULL);
      connectOptions.setHostnameVerificationAlgorithm("HTTPS");
      connectOptions.setPemTrustOptions(
        new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
      connectOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
      connectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    }
    connectOptions.setIdleTimeout(queryTimeout != null ? Integer.parseInt(queryTimeout) : DEFAULT_IDLE_TIMEOUT);
  }

  /**
   * Execute prepared query.
   *
   * @param sql   query
   * @param tuple tuple
   * @return async result rowset
   */
  public Future<RowSet<Row>> execute(String sql, Tuple tuple, String tenantId) {
    Future<Void> future = Future.succeededFuture();
    return future.compose(x -> preparedQuery(sql, tenantId).execute(tuple));
  }

  public PreparedQuery<RowSet<Row>> preparedQuery(String sql, String tenantId) {
    String preparedSql = replaceSchemaName(sql, tenantId);
    return getCachedPool(tenantId).preparedQuery(preparedSql);
  }

  public String getSchemaName(String tenantId) {
    return tenantId + "_" + MODULE_NAME;
  }

  private String replaceSchemaName(String sql, String tenantId) {
    return sql.replace("{schemaName}", getSchemaName(tenantId));
  }

  private static void close(PgPool client) {
    client.close();
  }

  public static void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }

}
