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

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.folio.inventory.rest.util.ModuleUtil.convertToPsqlStandard;
import static org.folio.inventory.rest.util.ModuleName.getModuleName;

public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger(PostgresClientFactory.class);

  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_USERNAME = "DB_USERNAME";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String DB_MAXPOOLSIZE = "DB_MAXPOOLSIZE";
  public static final String DB_SERVER_PEM = "DB_SERVER_PEM";
  public static final String DB_QUERYTIMEOUT = "DB_QUERYTIMEOUT";

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();
  private static final String MODULE_NAME = getModuleName();
  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";
  private static final String DEFAULT_IDLE_TIMEOUT = "60000";
  private static final String DEFAULT_MAX_POOL_SIZE = "5";

  private static Map<String, String> properties = System.getenv();

  static PgConnectOptions pgConnectOptions = new PgConnectOptions();

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   */
  private static boolean shouldResetPool = false;

  private Vertx vertx;

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
      return getPgPoolFromCache(tenantId);
    }
    if (shouldResetPool) {
      POOL_CACHE.remove(tenantId);
      shouldResetPool = false;
    }
    LOGGER.info("Creating new database connection pool for tenant {}.", tenantId);
    PgConnectOptions connectOptions = getConnectionOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(Integer.parseInt(getProperty(DB_MAXPOOLSIZE) != null ? getProperty(DB_MAXPOOLSIZE) : DEFAULT_MAX_POOL_SIZE));
    PgPool pgPool = PgPool.pool(vertx, connectOptions, poolOptions);
    putPgPoolToCache(tenantId, pgPool);

    return pgPool;
  }

  public static PgPool getPgPoolFromCache(String tenantId) {
    return POOL_CACHE.get(tenantId);
  }

  public static void putPgPoolToCache(String tenantId, PgPool pgPool) {
    POOL_CACHE.put(tenantId, pgPool);
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

  public static void setDefaultConnectionOptions(PgConnectOptions connectOptions) {
    PostgresClientFactory.pgConnectOptions = connectOptions;
  }

  /**
   * Get {@link PgConnectOptions}
   *
   * @param tenantId tenant id
   * @return postgres connection options
   */
  public static PgConnectOptions getConnectionOptions(String tenantId) {
    fillPgConnectOptions();
    if (tenantId != null) {
      pgConnectOptions.addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    }
    return pgConnectOptions;
  }

  public static void fillPgConnectOptions() {
    pgConnectOptions.getProperties().put("application_name", MODULE_NAME);
    if (getProperty(DB_HOST) != null) {
      pgConnectOptions.setHost(getProperty(DB_HOST));
    }
    if (getProperty(DB_PORT) != null) {
      pgConnectOptions.setPort(Integer.parseInt(getProperty(DB_PORT)));
    }
    if (getProperty(DB_DATABASE) != null) {
      pgConnectOptions.setDatabase(getProperty(DB_DATABASE));
    }
    if (getProperty(DB_USERNAME) != null) {
      pgConnectOptions.setUser(getProperty(DB_USERNAME));
    }
    if (getProperty(DB_PASSWORD) != null) {
      pgConnectOptions.setPassword(getProperty(DB_PASSWORD));
    }
    if (getProperty(DB_SERVER_PEM) != null) {
      pgConnectOptions.setSslMode(SslMode.VERIFY_FULL);
      pgConnectOptions.setHostnameVerificationAlgorithm("HTTPS");
      pgConnectOptions.setPemTrustOptions(
        new PemTrustOptions().addCertValue(Buffer.buffer(getProperty(DB_SERVER_PEM))));
      pgConnectOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
      pgConnectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    }
    pgConnectOptions.setIdleTimeout(Integer.parseInt(getProperty(DB_QUERYTIMEOUT) != null ? getProperty(DB_QUERYTIMEOUT) : DEFAULT_IDLE_TIMEOUT));
  }

  public static String getProperty(String key) {
    return properties.get(key);
  }

  public static void setProperties(Map<String, String> props) {
    properties = props;
  }

  public static Map<String, String> getProperties() {
    return properties;
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

  public void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }

}
