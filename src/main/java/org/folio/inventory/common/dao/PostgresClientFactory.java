package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.convertToPsqlStandard;

public class PostgresClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(PostgresClientFactory.class);

  private static final Map<String, Pool> POOL_CACHE = new HashMap<>();

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   */
  private static boolean shouldResetPool = false;

  private Vertx vertx;

  public PostgresClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Get {@link Pool}.
   *
   * @param tenantId tenant id.
   * @return pooled database client.
   */
  public Pool getCachedPool(String tenantId) {
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

  private Pool getCachedPool(Vertx vertx, String tenantId) {
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
    Pool pgPool = Pool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, pgPool);

    return pgPool;
  }

  private PreparedQuery<RowSet<Row>> preparedQuery(String sql, String tenantId) {
    String schemaName = convertToPsqlStandard(tenantId);
    String preparedSql = sql.replace("{schemaName}", schemaName);
    return getCachedPool(tenantId).preparedQuery(preparedSql);
  }

  /**
   * close all {@link Pool} clients.
   */
  public static Future<Void> closeAll() {
    List<Future<?>> closeFutures = POOL_CACHE.values()
      .stream()
      .map(SqlClient::close)
      .collect(Collectors.toList());

    return Future.all(closeFutures)
      .onSuccess(v -> {
        POOL_CACHE.clear();
        LOGGER.info("All SQL pools closed and cache cleared.");
      })
      .mapEmpty();
  }

  /**
   * For test usage only.
   */
  public void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }
}
