package org.folio.inventory.rest.impl;

import java.sql.Connection;
import java.sql.SQLException;

import io.vertx.pgclient.PgConnectOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.postgresql.ds.PGSimpleDataSource;

public class SingleConnectionProvider {
  private static final Logger LOGGER = LogManager.getLogger(SingleConnectionProvider.class);

  private static final String JDBC_DRIVER = "jdbc:postgresql";

  private SingleConnectionProvider() {
  }

  public static Connection getConnection(String tenantId) throws SQLException {
    LOGGER.info("Attempting to get connection for tenant {}", tenantId);
    PgConnectOptions connectOptions = PostgresConnectionOptions.getConnectionOptions(tenantId);
    return getConnectionInternal(connectOptions);
  }

  private static Connection getConnectionInternal(PgConnectOptions connectionConfig) throws SQLException {
    String host = connectionConfig.getHost();
    String port = String.valueOf(connectionConfig.getPort());
    String database = connectionConfig.getDatabase();
    String connectionUrl = String.format("%s://%s:%s/%s", JDBC_DRIVER, host, port, database);

    LOGGER.info("Attempting to get connection for url {}", connectionUrl);
    PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
    pgSimpleDataSource.setURL(connectionUrl);
    pgSimpleDataSource.setUser(connectionConfig.getUser());
    pgSimpleDataSource.setPassword(connectionConfig.getPassword());
    return pgSimpleDataSource.getConnection();
  }
}
