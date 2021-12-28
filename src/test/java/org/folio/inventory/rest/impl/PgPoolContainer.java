package org.folio.inventory.rest.impl;

import org.testcontainers.containers.PostgreSQLContainer;

public class PgPoolContainer {

  public static final String POSTGRES_IMAGE = "postgres:12-alpine";

  private static PostgreSQLContainer<?> container = new PostgreSQLContainer<>(POSTGRES_IMAGE);

  /**
   * Create PostgreSQL container for testing.
   */
  public static void create() {
    container.start();
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
