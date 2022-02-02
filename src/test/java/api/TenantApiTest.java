package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.vertx.pgclient.PgConnectOptions;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static api.ApiTestSuite.TENANT_ID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TenantApiTest extends ApiTests {

  @Test
  public void shouldCreateSchemaWithTables() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(200));
  }

  @Test
  public void shouldNotCreateSchemaWithTablesWithIncorrectConnectionOptions() throws Exception {
    PgConnectOptions pgConnectOptions = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    Map<String, String> systemProperties = Map.of(DB_HOST, pgConnectOptions.getHost(),
      DB_DATABASE, pgConnectOptions.getDatabase(),
      DB_PORT, String.valueOf(pgConnectOptions.getPort()),
      DB_USERNAME, pgConnectOptions.getUser(),
      DB_PASSWORD, pgConnectOptions.getPassword());
    PostgresConnectionOptions.setSystemProperties(new HashMap<>());

    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(500));

    PostgresConnectionOptions.setSystemProperties(systemProperties);
  }

  @Test
  public void shouldCreateAndDeleteSchema() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(200));

    final var deleteCompleted = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponse.getStatusCode(), is(200));
  }

  @Test
  public void shouldNotDropSchemaWithIncorrectConnectionOptions() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(200));

    PgConnectOptions pgConnectOptions = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    Map<String, String> systemProperties = Map.of(DB_HOST, pgConnectOptions.getHost(),
      DB_DATABASE, pgConnectOptions.getDatabase(),
      DB_PORT, String.valueOf(pgConnectOptions.getPort()),
      DB_USERNAME, pgConnectOptions.getUser(),
      DB_PASSWORD, pgConnectOptions.getPassword());
    PostgresConnectionOptions.setSystemProperties(new HashMap<>());

    final var deleteCompletedBefore = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponseBefore = deleteCompletedBefore.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponseBefore.getStatusCode(), is(500));

    PostgresConnectionOptions.setSystemProperties(systemProperties);

    final var deleteCompletedAfter = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponseAfter = deleteCompletedAfter.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponseAfter.getStatusCode(), is(200));
  }

}
