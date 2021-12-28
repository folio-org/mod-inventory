package org.folio.inventory.common.dao;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import org.apache.commons.lang3.StringUtils;
import static org.folio.inventory.rest.util.ModuleName.getModuleName;
import static org.folio.inventory.rest.util.ModuleUtil.convertToPsqlStandard;

import java.util.Collections;
import java.util.Map;

/**
 * Utility class to get connection properties used to connect to Postgres DB.
 */
public class PostgresConnectionOptions {
  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";
  private static final String DEFAULT_IDLE_TIMEOUT = "60000";
  private static final String DEFAULT_MAX_POOL_SIZE = "5";
  private static final String MODULE_NAME = getModuleName();

  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_USERNAME = "DB_USERNAME";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String DB_MAXPOOLSIZE = "DB_MAXPOOLSIZE";
  public static final String DB_SERVER_PEM = "DB_SERVER_PEM";
  public static final String DB_QUERYTIMEOUT = "DB_QUERYTIMEOUT";

  private Map<String, String> systemProperties;

  public PostgresConnectionOptions(Map<String, String> systemProperties) {
    this.systemProperties = systemProperties;
  }

  /**
   * Get {@link PgConnectOptions}
   *
   * @param tenantId tenant id
   * @return postgres connection options
   */
  public PgConnectOptions getConnectionOptions(String tenantId) {
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
      pgConnectionOptions.setHostnameVerificationAlgorithm("HTTPS");
      pgConnectionOptions.setPemTrustOptions(
        new PemTrustOptions().addCertValue(Buffer.buffer(getSystemProperty(DB_SERVER_PEM))));
      pgConnectionOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
      pgConnectionOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    }
    pgConnectionOptions.setIdleTimeout(Integer.parseInt(getSystemProperty(DB_QUERYTIMEOUT) != null ? getSystemProperty(DB_QUERYTIMEOUT) : DEFAULT_IDLE_TIMEOUT));
    if (StringUtils.isNotBlank(tenantId)) {
      pgConnectionOptions.addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    }
    return pgConnectionOptions;
  }

  public Integer getMaxPoolSize() {
    return Integer.parseInt(getSystemProperty(DB_MAXPOOLSIZE) != null ? getSystemProperty(DB_MAXPOOLSIZE) : DEFAULT_MAX_POOL_SIZE);
  }

  public String getSystemProperty(String key) {
    return systemProperties.get(key);
  }

}
