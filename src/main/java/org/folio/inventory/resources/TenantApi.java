package org.folio.inventory.resources;

import static java.lang.String.format;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.convertToPsqlStandard;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.rest.impl.SingleConnectionProvider;
import org.folio.inventory.services.InventoryKafkaTopicService;
import org.folio.kafka.services.KafkaAdminClientService;

public class TenantApi {
  private static final Logger LOGGER = LogManager.getLogger(TenantApi.class);

  private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";
  private static final String DROP_SCHEMA_SQL = "DROP SCHEMA IF EXISTS %s CASCADE";
  private static final String CHANGELOG_TENANT_PATH = "liquibase/tenant/changelog.xml";
  private static final String TENANT_PATH = "/_/tenant";
  private InventoryKafkaTopicService inventoryKafkaTopicService = new InventoryKafkaTopicService();

  public void register(Router router) {
    router.post(TENANT_PATH).handler(this::create);
    router.delete(TENANT_PATH).handler(this::delete);
  }

  public void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    initializeSchemaForTenant(context.getTenantId())
      .onSuccess(result -> {
          Vertx vertx = routingContext.vertx();
          var kafkaAdminClientService = new KafkaAdminClientService(vertx);
          kafkaAdminClientService.createKafkaTopics(inventoryKafkaTopicService.createTopicObjects(), context.getTenantId());
          routingContext.response().setStatusCode(HttpStatus.HTTP_NO_CONTENT.toInt()).end();
        }
      )
      .onFailure(fail -> routingContext.response().setStatusCode(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()).end(fail.toString()));
  }

  public void delete(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    deleteSchemaForTenant(context.getTenantId())
      .onSuccess(result -> routingContext.response().setStatusCode(HttpStatus.HTTP_NO_CONTENT.toInt()).end())
      .onFailure(fail -> routingContext.response().setStatusCode(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()).end(fail.toString()));
  }

  public Future<Integer> initializeSchemaForTenant(String tenantId) {
    String schemaName = convertToPsqlStandard(tenantId);
    LOGGER.info("Initializing schema {} for tenant {}", schemaName, tenantId);

    SingleConnectionProvider connectionProvider = new SingleConnectionProvider();

    try (var connection = connectionProvider.getConnection(tenantId);
         var preparedStatement = connection.prepareStatement(format(CREATE_SCHEMA_SQL, schemaName))) {
      boolean schemaIsNotExecuted = preparedStatement.execute();
      if (schemaIsNotExecuted) {
        return Future.failedFuture(String.format("Cannot create schema %s", schemaName));
      } else {
        LOGGER.info("Schema {} created or already exists", schemaName);
        runScripts(schemaName, connection);
        LOGGER.info("Schema is initialized for tenant {}", tenantId);
        return Future.succeededFuture(0);
      }
    } catch (Exception e) {
      String cause = format("Error while initializing schema %s for tenant %s", schemaName, tenantId);
      LOGGER.error(cause, e);
      return Future.failedFuture(cause);
    }
  }

  public Future<Integer> deleteSchemaForTenant(String tenantId) {
    String schemaName = convertToPsqlStandard(tenantId);
    LOGGER.info("Attempting to drop schema {} for tenant {}", schemaName, tenantId);

    SingleConnectionProvider connectionProvider = new SingleConnectionProvider();

    try (var connection = connectionProvider.getConnection(tenantId);
         var preparedStatement = connection.prepareStatement(format(DROP_SCHEMA_SQL, schemaName))) {
      boolean schemaIsNotDeleted = preparedStatement.execute();
      if (schemaIsNotDeleted) {
        return Future.failedFuture(String.format("Cannot drop schema %s", schemaName));
      } else {
        LOGGER.info("Drop action for schema {} was successful", schemaName);
        return Future.succeededFuture(0);
      }
    } catch (Exception e) {
      String cause = format("Error attempting to drop schema %s for tenant %s", schemaName, tenantId);
      LOGGER.error(cause, e);
      return Future.failedFuture(cause);
    }
  }

  private void runScripts(String schemaName, Connection connection) throws LiquibaseException {
    var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
    database.setDefaultSchemaName(schemaName);
    try (var liquibase = new Liquibase(CHANGELOG_TENANT_PATH, new ClassLoaderResourceAccessor(), database)) {
      liquibase.update(new Contexts());
    }
    // no need to call database.close() because liquibase.close() has already closed the connection.
  }
}
