package api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;

class InventoryConfigApiTest extends ApiTests {

  private static final InventoryConfiguration CONFIG = new InventoryConfigurationImpl();

  @SneakyThrows
  @Test
  void shouldReturnInstanceBlockedFieldsConfig() {
    final var getCompleted = okapiClient.get(ApiRoot.instanceBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : CONFIG.getInstanceBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }

  @SneakyThrows
  @Test
  void shouldReturnHoldingsBlockedFieldsConfig() {
    final var getCompleted = okapiClient.get(ApiRoot.holdingsBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : CONFIG.getHoldingsBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }
}
