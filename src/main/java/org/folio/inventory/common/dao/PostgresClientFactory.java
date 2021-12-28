package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static org.folio.inventory.rest.util.ModuleName.getModuleName;

public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger(PostgresClientFactory.class);

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();
  private static final String MODULE_NAME = getModuleName();

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   */
  private static boolean shouldResetPool = false;

  private Vertx vertx;

  public PostgresClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Get {@link PgPool}.
   *
   * @param tenantId tenant id.
   * @return pooled database client.
   */
  public PgPool getCachedPool(String tenantId) {
    return getCachedPool(this.vertx, tenantId);
  }

  /**
   * Execute prepared query.
   *
   * @param sql   query.
   * @param tuple tuple.
   * @return async result rowset.
   */
  public Future<RowSet<Row>> execute(String sql, Tuple tuple, String tenantId) {
    Future<Void> future = Future.succeededFuture();
    return future.compose(x -> preparedQuery(sql, tenantId).execute(tuple));
  }

  private PgPool getCachedPool(Vertx vertx, String tenantId) {
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
    PgConnectOptions connectOptions = PostgresConnectionOptions.getConnectionOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(PostgresConnectionOptions.getMaxPoolSize());
    PgPool pgPool = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, pgPool);

    return pgPool;
  }

  private PreparedQuery<RowSet<Row>> preparedQuery(String sql, String tenantId) {
    String preparedSql = replaceSchemaName(sql, tenantId);
    return getCachedPool(tenantId).preparedQuery(preparedSql);
  }

  private String replaceSchemaName(String sql, String tenantId) {
    return sql.replace("{schemaName}", getSchemaName(tenantId));
  }

  private String getSchemaName(String tenantId) {
    return tenantId + "_" + MODULE_NAME;
  }

  /**
   * close all {@link PgPool} clients
   */
  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
  }

  private static void close(PgPool client) {
    client.close();
  }

  /**
   * For test usage only.
   * @param shouldResetPool
   */
  public void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }

}
