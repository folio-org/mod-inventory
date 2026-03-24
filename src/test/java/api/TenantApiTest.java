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
import static org.folio.HttpStatus.HTTP_INTERNAL_SERVER_ERROR;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_DATABASE;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_HOST;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PASSWORD;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PORT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_USERNAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TenantApiTest extends ApiTests {

  public static int NO_CONTENT_STATUS = HTTP_NO_CONTENT.toInt();
  public static int INTERNAL_SERVER_ERROR_STATUS = HTTP_INTERNAL_SERVER_ERROR.toInt();

  @Test
  public void shouldCreateSchemaWithTables() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(NO_CONTENT_STATUS));
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
    assertThat(postResponse.getStatusCode(), is(INTERNAL_SERVER_ERROR_STATUS));

    PostgresConnectionOptions.setSystemProperties(systemProperties);
  }

  @Test
  public void shouldCreateAndDeleteSchema() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(HTTP_NO_CONTENT.toInt()));

    final var deleteCompleted = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponse.getStatusCode(), is(NO_CONTENT_STATUS));
  }

  @Test
  public void shouldNotDropSchemaWithIncorrectConnectionOptions() throws Exception {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(NO_CONTENT_STATUS));

    PgConnectOptions pgConnectOptions = PostgresConnectionOptions.getConnectionOptions(TENANT_ID);
    Map<String, String> systemProperties = Map.of(DB_HOST, pgConnectOptions.getHost(),
      DB_DATABASE, pgConnectOptions.getDatabase(),
      DB_PORT, String.valueOf(pgConnectOptions.getPort()),
      DB_USERNAME, pgConnectOptions.getUser(),
      DB_PASSWORD, pgConnectOptions.getPassword());
    PostgresConnectionOptions.setSystemProperties(Map.of(DB_HOST, "invalid", DB_PORT, "999999"));

    final var deleteCompletedBefore = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponseBefore = deleteCompletedBefore.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponseBefore.getStatusCode(), is(INTERNAL_SERVER_ERROR_STATUS));

    PostgresConnectionOptions.setSystemProperties(systemProperties);

    final var deleteCompletedAfter = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponseAfter = deleteCompletedAfter.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponseAfter.getStatusCode(), is(NO_CONTENT_STATUS));
  }

}
