package org.folio.inventory.rest.impl;

import io.vertx.pgclient.PgConnectOptions;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

public class PgPoolContainer {

  public static final String POSTGRES_IMAGE = "postgres:12-alpine";

  private static PostgreSQLContainer<?> container = new PostgreSQLContainer<>(POSTGRES_IMAGE);

  /**
   * Create PostgreSQL container for testing.
   */
  public static void create() {
    container.start();

    PostgresClientFactory.setDefaultConnectionOptions(new PgConnectOptions()
      .setPort(container.getFirstMappedPort())
      .setHost(container.getHost())
      .setDatabase(container.getDatabaseName())
      .setUser(container.getUsername())
      .setPassword(container.getPassword())
    );
  }

  /**
   * Stop PostgreSQL container.
   */
  public static void stop() {
    container.stop();
  }

  public static boolean isRunning() {
    return container.isRunning();
  }
}
