package org.folio.inventory.common.dao;

import static java.lang.String.format;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;

/**
 * Resolves the Postgres connection properties used to connect to the database.
 *
 * <p>Values are read from an environment source ({@link System#getenv()} by default). Supply an
 * explicit {@link Map} to point the module at a different environment (e.g. an embedded database in
 * tests), or use {@link #fromConfig(JsonObject)} to let Vert.x deployment config override the
 * environment on a per-key basis.
 */
public class PostgresConnectionOptions {
  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_USERNAME = "DB_USERNAME";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String DB_MAXPOOLSIZE = "DB_MAXPOOLSIZE";
  public static final String DB_SERVER_PEM = "DB_SERVER_PEM";
  public static final String DB_IDLETIMEOUT = "DB_IDLETIMEOUT";
  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";
  private static final String DEFAULT_IDLE_TIMEOUT = "60000";
  private static final String DEFAULT_MAX_POOL_SIZE = "5";
  private static final String MODULE_NAME = "mod_inventory";
  private static final List<String> DB_KEYS = List.of(DB_HOST, DB_PORT, DB_DATABASE, DB_USERNAME,
    DB_PASSWORD, DB_MAXPOOLSIZE, DB_SERVER_PEM, DB_IDLETIMEOUT);

  private final Map<String, String> environment;

  /**
   * Read connection properties from the process environment ({@link System#getenv()}).
   */
  public PostgresConnectionOptions() {
    this(System.getenv());
  }

  /**
   * Read connection properties from the supplied environment.
   *
   * @param environment map of {@code DB_*} properties to connection values.
   */
  public PostgresConnectionOptions(Map<String, String> environment) {
    this.environment = environment;
  }

  /**
   * Build options from Vert.x deployment {@code config}, falling back to the process environment for
   * any {@code DB_*} key not present in the config.
   *
   * @param config verticle deployment configuration.
   * @return connection options resolved from config overlaid on the environment.
   */
  public static PostgresConnectionOptions fromConfig(JsonObject config) {
    var environment = new HashMap<>(System.getenv());
    DB_KEYS.forEach(key -> {
      if (config.containsKey(key)) {
        environment.put(key, config.getString(key));
      }
    });
    return new PostgresConnectionOptions(environment);
  }

  /**
   * Get {@link PgConnectOptions}.
   *
   * @param tenantId tenant id.
   * @return postgres connection options.
   */
  public PgConnectOptions getConnectionOptions(String tenantId) {
    PgConnectOptions pgConnectionOptions = new PgConnectOptions();
    pgConnectionOptions.getProperties().put("application_name", MODULE_NAME);

    applyIfPresent(DB_HOST, pgConnectionOptions::setHost);
    applyIfPresent(DB_PORT, value -> pgConnectionOptions.setPort(Integer.parseInt(value)));
    applyIfPresent(DB_DATABASE, pgConnectionOptions::setDatabase);
    applyIfPresent(DB_USERNAME, pgConnectionOptions::setUser);
    applyIfPresent(DB_PASSWORD, pgConnectionOptions::setPassword);
    applyIfPresent(DB_SERVER_PEM, pem -> {
      pgConnectionOptions.setSslMode(SslMode.VERIFY_FULL);
      pgConnectionOptions.setSslOptions(new ClientSSLOptions()
        .setHostnameVerificationAlgorithm("HTTPS")
        .setTrustOptions(new PemTrustOptions().addCertValue(Buffer.buffer(pem)))
        .setEnabledSecureTransportProtocols(Set.of("TLSv1.3")));
    });

    if (StringUtils.isNotBlank(tenantId)) {
      pgConnectionOptions.addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    }
    return pgConnectionOptions;
  }

  public PoolOptions getPoolOptions() {
    return new PoolOptions()
      .setMaxSize(getMaxPoolSize())
      .setIdleTimeout(Integer.parseInt(getOrDefault(DB_IDLETIMEOUT, DEFAULT_IDLE_TIMEOUT)))
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS);
  }

  public Integer getMaxPoolSize() {
    return Integer.parseInt(getOrDefault(DB_MAXPOOLSIZE, DEFAULT_MAX_POOL_SIZE));
  }

  /**
   * RMB convention driven tenant to schema name.
   *
   * @param tenantId tenant id.
   * @return formatted schema and module name.
   */
  public static String convertToPsqlStandard(String tenantId) {
    return format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

  private void applyIfPresent(String key, Consumer<String> setter) {
    String value = environment.get(key);
    if (StringUtils.isNotBlank(value)) {
      setter.accept(value);
    }
  }

  private String getOrDefault(String key, String defaultValue) {
    String value = environment.get(key);
    return StringUtils.isNotBlank(value) ? value : defaultValue;
  }
}
