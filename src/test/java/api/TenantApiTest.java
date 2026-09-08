package api;

import static api.ApiTestSuite.TENANT_ID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_HOST;
import static org.folio.inventory.common.dao.PostgresConnectionOptions.DB_PORT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.folio.inventory.common.dao.PostgresConnectionOptions;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;
import support.PgPoolContainer;

class TenantApiTest extends ApiTests {

  @Test
  void shouldCreateSchemaWithTables() throws Exception {
    final var postCompleted = okapiClient.post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.statusCode(), is(HTTP_NO_CONTENT.toInt()));
  }

  @DisplayName("should fail schema creation when connection options are incorrect")
  @Test
  void shouldNotCreateSchema_whenConnectionOptionsAreIncorrect() {
    // given
    var tenantApi = new TenantApi(new PostgresConnectionOptions(new HashMap<>()));

    // when
    var result = tenantApi.initializeSchemaForTenant(TENANT_ID);

    // then
    assertThat(result.failed(), is(true));
  }

  @Test
  void shouldCreateAndDeleteSchema() throws Exception {
    final var postCompleted = okapiClient.post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.statusCode(), is(HTTP_NO_CONTENT.toInt()));

    final var deleteCompleted = okapiClient
      .delete(ApiRoot.tenant());

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(deleteResponse.statusCode(), is(HTTP_NO_CONTENT.toInt()));
  }

  @DisplayName("should fail schema drop when connection options are incorrect")
  @Test
  void shouldNotDropSchema_whenConnectionOptionsAreIncorrect() {
    // given
    var validTenantApi = new TenantApi(new PostgresConnectionOptions(PgPoolContainer.getConnectionEnv()));
    var invalidTenantApi =
      new TenantApi(new PostgresConnectionOptions(Map.of(DB_HOST, "invalid", DB_PORT, "999999")));
    validTenantApi.initializeSchemaForTenant(TENANT_ID);

    // when
    var failedDrop = invalidTenantApi.deleteSchemaForTenant(TENANT_ID);
    var successfulDrop = validTenantApi.deleteSchemaForTenant(TENANT_ID);

    // then
    assertThat(failedDrop.failed(), is(true));
    assertThat(successfulDrop.succeeded(), is(true));
  }
}
