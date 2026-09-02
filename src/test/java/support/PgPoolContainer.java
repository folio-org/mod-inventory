package support;

import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_DATABASE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_HOST;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PASSWORD;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PORT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_USERNAME;

import java.util.Map;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgPoolContainer {

  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine";
  public static final String POSTGRES_IMAGE = System.getenv()
    .getOrDefault("TESTCONTAINERS_POSTGRES_IMAGE", DEFAULT_POSTGRES_IMAGE);

  private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer(POSTGRES_IMAGE);

  /**
   * Create PostgreSQL container for testing.
   */
  public static void create() {
    CONTAINER.start();

    setEmbeddedPostgresOptions();
  }

  /**
   * Set embedded container system properties.
   */
  public static void setEmbeddedPostgresOptions() {
    if (isRunning()) {
      Map<String, String> systemProperties = Map.of(DB_HOST, CONTAINER.getHost(),
        DB_DATABASE, CONTAINER.getDatabaseName(),
        DB_USERNAME, CONTAINER.getUsername(),
        DB_PASSWORD, CONTAINER.getPassword(),
        DB_PORT, String.valueOf(CONTAINER.getFirstMappedPort()));
      PostgresConnectionOptions.setSystemProperties(systemProperties);
    }
  }

  /**
   * Stop PostgreSQL container.
   */
  public static void stop() {
    CONTAINER.stop();
  }

  /**
   * Check if embedded container is already running.
   *
   * @return embedded container is running.
   */
  public static boolean isRunning() {
    return CONTAINER.isRunning();
  }
}
