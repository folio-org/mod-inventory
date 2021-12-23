package org.folio.inventory.rest.impl;

import io.vertx.pgclient.PgConnectOptions;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

public class PgPoolContainer {

  public static final String POSTGRES_IMAGE = "postgres:12-alpine";

  private static PostgreSQLContainer<?> container;

  /**
   * Create PostgreSQL container for testing.
   * @return container.
   */
  public static PostgreSQLContainer<?> create() {
    return create(POSTGRES_IMAGE);
  }

  /**
   * Create PostgreSQL container for testing.
   * @param image container image name.
   * @return container.
   */
  public static PostgreSQLContainer<?> create(String image) {
    container = new PostgreSQLContainer<>(image);
    container.start();

    PostgresClientFactory.setDefaultConnectionOptions(new PgConnectOptions()
      .setPort(container.getFirstMappedPort())
      .setHost(container.getHost())
      .setDatabase(container.getDatabaseName())
      .setUser(container.getUsername())
      .setPassword(container.getPassword())
    );

    return container;
  }
}
