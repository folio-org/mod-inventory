package org.folio.inventory.resources;

import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebContext;

import java.sql.Connection;

import static java.lang.String.format;
import static org.folio.inventory.rest.util.ModuleUtil.convertToPsqlStandard;
import static org.folio.inventory.rest.impl.SingleConnectionProvider.getConnection;


public class SchemaApi {

  private static final Logger LOGGER = LogManager.getLogger(SchemaApi.class);

  private static final String CHANGELOG_TENANT_PATH = "liquibase/tenant/changelog.xml";
  private static final String TENANT_PATH = "/_/tenant";

  public void register(Router router) {
    router.post(TENANT_PATH).handler(this::create);
  }

  public void create(RoutingContext routingContext) {

    WebContext context = new WebContext(routingContext);

    loadData(context.getTenantId())
      .onComplete(res -> {
        routingContext.response().setStatusCode(200).end();
      });
  }

  Future<Integer> loadData(String tenantId) {
    return Future.succeededFuture(0).compose(num -> {
      initializeSchemaForTenant(tenantId);
      return Future.succeededFuture(num);
    });
  }

  public static void initializeSchemaForTenant(String tenant) {
    String schemaName = convertToPsqlStandard(tenant);
    LOGGER.info(format("Initializing schema %s for tenant %s", schemaName, tenant));
    try (Connection connection = getConnection(tenant)) {
      runScripts(schemaName, connection, CHANGELOG_TENANT_PATH);
      LOGGER.info("Schema is initialized for tenant " + tenant);
    } catch (Exception e) {
      LOGGER.error(format("Error while initializing schema %s for tenant %s", schemaName, tenant), e);
    }
  }

  private static void runScripts(String schemaName, Connection connection, String changelogPath) throws LiquibaseException {
    Liquibase liquibase = null;
    LOGGER.info("Schema name {} " + schemaName);
    try {
      Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
      database.setDefaultSchemaName(schemaName);
      liquibase = new Liquibase(changelogPath, new ClassLoaderResourceAccessor(), database);
      liquibase.update(new Contexts());
    } finally {
      if (liquibase != null && liquibase.getDatabase() != null) {
        Database database = liquibase.getDatabase();
        database.close();
      }
    }
  }
}
