package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class InventoryConfigApiTest extends ApiTests {
  private static final InventoryConfiguration config = new InventoryConfigurationImpl();

  @Test
  public void shouldReturnInstanceBlockedFieldsConfig() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var getCompleted = okapiClient.get(ApiRoot.instanceBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : config.getInstanceBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }

  @Test
  public void shouldReturnHoldingsBlockedFieldsConfig() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var getCompleted = okapiClient.get(ApiRoot.holdingsBlockedFieldsConfig());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject actualResponse = getResponse.getJson();

    for (String blockedField : config.getHoldingsBlockedFields()) {
      assertTrue(actualResponse.getJsonArray("blockedFields").contains(blockedField));
    }
  }
}
