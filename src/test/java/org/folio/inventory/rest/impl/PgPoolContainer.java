package org.folio.inventory.rest.impl;

import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Map;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.*;

public class PgPoolContainer {

  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine";
  public static final String POSTGRES_IMAGE =
      System.getenv().getOrDefault("TESTCONTAINERS_POSTGRES_IMAGE", DEFAULT_POSTGRES_IMAGE);

  private static PostgreSQLContainer<?> container = new PostgreSQLContainer<>(POSTGRES_IMAGE);

  /**
   * Create PostgreSQL container for testing.
   */
  public static void create() {
    container.start();

    setEmbeddedPostgresOptions();
  }

  /**
   * Set embedded container system properties.
   */
  public static void setEmbeddedPostgresOptions() {
    if (isRunning()) {
      Map<String, String> systemProperties = Map.of(DB_HOST, container.getHost(),
        DB_DATABASE, container.getDatabaseName(),
        DB_USERNAME, container.getUsername(),
        DB_PASSWORD, container.getPassword(),
        DB_PORT, String.valueOf(container.getFirstMappedPort()));
      PostgresConnectionOptions.setSystemProperties(systemProperties);
    }
  }

  /**
   * Stop PostgreSQL container.
   */
  public static void stop() {
    container.stop();
  }

  /**
   * Check if embedded container is already running.
   * @return embedded container is running.
   */
  public static boolean isRunning() {
    return container.isRunning();
  }
}
