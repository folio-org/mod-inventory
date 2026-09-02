package api;

import support.ApiRoot;
import support.ApiTests;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InventoryConfigApiTest extends ApiTests {

  private static final InventoryConfiguration config = new InventoryConfigurationImpl();

  @SneakyThrows
  @Test
  void shouldReturnInstanceBlockedFieldsConfig() {
    final var getCompleted = okapiClient.get(ApiRoot.instanceBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : config.getInstanceBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }

  @SneakyThrows
  @Test
  void shouldReturnHoldingsBlockedFieldsConfig() {
    final var getCompleted = okapiClient.get(ApiRoot.holdingsBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : config.getHoldingsBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }
}
