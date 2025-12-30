package org.folio.inventory.common.dao;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import static java.lang.String.format;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to get connection properties used to connect to Postgres DB.
 */
public class PostgresConnectionOptions {
  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";
  private static final String DEFAULT_IDLE_TIMEOUT = "60000";
  private static final String DEFAULT_MAX_POOL_SIZE = "5";
  private static final String MODULE_NAME = "mod_inventory";

  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_USERNAME = "DB_USERNAME";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String DB_MAXPOOLSIZE = "DB_MAXPOOLSIZE";
  public static final String DB_SERVER_PEM = "DB_SERVER_PEM";
  public static final String DB_IDLETIMEOUT = "DB_IDLETIMEOUT";

  /**
   * -- SETTER --
   *  For test usage only.
   *
   * @param newSystemProperties Map of system properties to set.
   */
  @Setter
  private static Map<String, String> systemProperties = System.getenv();

  private PostgresConnectionOptions() {

  }

  /**
   * Get {@link PgConnectOptions}.
   *
   * @param tenantId tenant id.
   * @return postgres connection options.
   */
  public static PgConnectOptions getConnectionOptions(String tenantId) {
    PgConnectOptions pgConnectionOptions = new PgConnectOptions();

    pgConnectionOptions.getProperties().put("application_name", MODULE_NAME);
    if (StringUtils.isNotBlank(getSystemProperty(DB_HOST))) {
      pgConnectionOptions.setHost(getSystemProperty(DB_HOST));
    }
    if (StringUtils.isNotBlank(getSystemProperty(DB_PORT))) {
      pgConnectionOptions.setPort(Integer.parseInt(getSystemProperty(DB_PORT)));
    }
    if (StringUtils.isNotBlank(getSystemProperty(DB_DATABASE))) {
      pgConnectionOptions.setDatabase(getSystemProperty(DB_DATABASE));
    }
    if (StringUtils.isNotBlank(getSystemProperty(DB_USERNAME))) {
      pgConnectionOptions.setUser(getSystemProperty(DB_USERNAME));
    }
    if (StringUtils.isNotBlank(getSystemProperty(DB_PASSWORD))) {
      pgConnectionOptions.setPassword(getSystemProperty(DB_PASSWORD));
    }

    if (StringUtils.isNotBlank(getSystemProperty(DB_SERVER_PEM))) {
      pgConnectionOptions.setSslMode(SslMode.VERIFY_FULL);

      ClientSSLOptions sslClientOptions = new ClientSSLOptions()
        .setHostnameVerificationAlgorithm("HTTPS")
        .setTrustOptions(new PemTrustOptions().addCertValue(Buffer.buffer(getSystemProperty(DB_SERVER_PEM))))
        .setEnabledSecureTransportProtocols(Set.of("TLSv1.3"));
      pgConnectionOptions.setSslOptions(sslClientOptions);
      //pgConnectionOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    }
    if (StringUtils.isNotBlank(tenantId)) {
      pgConnectionOptions.addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    }
    return pgConnectionOptions;
  }

  public static PoolOptions getPoolOptions() {
    return new PoolOptions()
      .setMaxSize(PostgresConnectionOptions.getMaxPoolSize())
      .setIdleTimeout(Integer.parseInt(StringUtils.isNotBlank(getSystemProperty(DB_IDLETIMEOUT)) ? getSystemProperty(DB_IDLETIMEOUT) : DEFAULT_IDLE_TIMEOUT))
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS);
  }

  public static Integer getMaxPoolSize() {
    return Integer.parseInt(getSystemProperty(DB_MAXPOOLSIZE) != null ? getSystemProperty(DB_MAXPOOLSIZE) : DEFAULT_MAX_POOL_SIZE);
  }

  public static String getSystemProperty(String key) {
    return systemProperties.get(key);
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

}
