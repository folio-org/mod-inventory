package org.folio.inventory.common.dao;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.convertToPsqlStandard;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger(PostgresClientFactory.class);

  private static final Map<String, Pool> POOL_CACHE = new HashMap<>();

  private final Vertx vertx;
  private final PostgresConnectionOptions connectionOptions;

  public PostgresClientFactory(Vertx vertx) {
    this(vertx, new PostgresConnectionOptions());
  }

  public PostgresClientFactory(Vertx vertx, PostgresConnectionOptions connectionOptions) {
    this.vertx = vertx;
    this.connectionOptions = connectionOptions;
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

  /**
   * close all {@link Pool} clients.
   */
  public static Future<Void> closeAll() {
    List<Future<Void>> closeFutures = List.copyOf(POOL_CACHE.keySet())
      .stream()
      .map(PostgresClientFactory::closePool)
      .toList();

    return Future.all(closeFutures)
      .onSuccess(v -> LOGGER.info("All SQL pools closed and cache cleared."))
      .mapEmpty();
  }

  /**
   * Close and evict the cached {@link Pool} for a single tenant.
   *
   * @param tenantId tenant id.
   * @return future completed when the pool is closed (or immediately if none was cached).
   */
  public static Future<Void> closePool(String tenantId) {
    Pool pool = POOL_CACHE.remove(tenantId);
    if (pool == null) {
      return Future.succeededFuture();
    }
    try {
      return pool.close()
        .onSuccess(v -> LOGGER.info("Closed database connection pool for tenant {}.", tenantId))
        .otherwise(err -> {
          LOGGER.warn("Failed to close pool for tenant {}: {}", tenantId, err.getMessage());
          return null;
        });
    } catch (RejectedExecutionException e) {
      // The pool is bound to an already-terminated Vert.x; it is effectively closed.
      LOGGER.warn("Pool for tenant {} was bound to a terminated Vert.x; evicted without closing.", tenantId);
      return Future.succeededFuture();
    }
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

  private Pool getCachedPool(Vertx vertx, String tenantId) {
    // assumes a single-threaded Vert.x model, so no synchronization needed
    return POOL_CACHE.computeIfAbsent(tenantId, id -> createPool(vertx, id));
  }

  private Pool createPool(Vertx vertx, String tenantId) {
    LOGGER.info("Creating new database connection pool for tenant {}.", tenantId);
    PgConnectOptions connectOptions = connectionOptions.getConnectionOptions(tenantId);
    return Pool.pool(vertx, connectOptions, connectionOptions.getPoolOptions());
  }

  private PreparedQuery<RowSet<Row>> preparedQuery(String sql, String tenantId) {
    String schemaName = convertToPsqlStandard(tenantId);
    String preparedSql = sql.replace("{schemaName}", schemaName);
    return getCachedPool(tenantId).preparedQuery(preparedSql);
  }
}
