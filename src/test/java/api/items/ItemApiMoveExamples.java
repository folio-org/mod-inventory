package api.items;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.builders.ItemsMoveRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class ItemApiMoveExamples extends ApiTests {

  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

  @Test
  public void canCreateItemsMove() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID instanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(instanceId));

    final UUID existedHoldingId = createHoldingForInstance(instanceId);
    final UUID newHoldingId = createHoldingForInstance(instanceId);

    Assert.assertNotEquals(existedHoldingId, newHoldingId);

    final IndividualResource createItem1 = itemsClient.create(new ItemRequestBuilder().forHolding(existedHoldingId)
      .withBarcode("645398607547")
      .withStatus(ItemStatusName.AVAILABLE.value()));

    final IndividualResource createItem2 = itemsClient.create(new ItemRequestBuilder().forHolding(existedHoldingId)
      .withBarcode("645398607546")
      .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsMoveRequestBody = new ItemsMoveRequestBuilder(newHoldingId,
        new JsonArray(Arrays.asList(createItem1.getId(), createItem2.getId()))).create();

    CompletableFuture<Response> postItemsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.move(), itemsMoveRequestBody, ResponseHandler.any(postItemsMoveCompleted));
    Response postItemsMoveResponse = postItemsMoveCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postItemsMoveResponse.getStatusCode(), is(201));
    assertThat(postItemsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject updatedItem1 = itemsClient.getById(createItem1.getId())
      .getJson();
    JsonObject updatedItem2 = itemsClient.getById(createItem2.getId())
      .getJson();

    Assert.assertEquals(newHoldingId.toString(), updatedItem1.getString(HOLDINGS_RECORD_ID));
    Assert.assertEquals(newHoldingId.toString(), updatedItem2.getString(HOLDINGS_RECORD_ID));
  }

  @Test
  public void canCreatePartialItemsMove() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID instanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(instanceId));

    final UUID existedHoldingId = createHoldingForInstance(instanceId);
    final UUID newHoldingId = createHoldingForInstance(instanceId);

    Assert.assertNotEquals(existedHoldingId, newHoldingId);

    final UUID nonExistedItemId = UUID.randomUUID();

    final IndividualResource createItem1 = itemsClient.create(new ItemRequestBuilder().forHolding(existedHoldingId)
      .withBarcode("645398607547")
      .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsMoveRequestBody = new ItemsMoveRequestBuilder(newHoldingId,
        new JsonArray(Arrays.asList(createItem1.getId(), nonExistedItemId))).create();

    CompletableFuture<Response> postItemsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.move(), itemsMoveRequestBody, ResponseHandler.any(postItemsMoveCompleted));
    Response postItemsMoveResponse = postItemsMoveCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postItemsMoveResponse.getStatusCode(), is(404));
    assertThat(postItemsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    List notFoundIds = postItemsMoveResponse.getJson()
      .getJsonObject("errors")
      .getJsonArray("ids")
      .getList();
    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.get(0), equalTo(nonExistedItemId.toString()));

    JsonObject updatedItem1 = itemsClient.getById(createItem1.getId())
      .getJson();
    assertThat(newHoldingId.toString(), equalTo(updatedItem1.getString(HOLDINGS_RECORD_ID)));
  }

  @Test
  public void cannotCreateMoveWithoutToHoldingsRecordId()
      throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    CompletableFuture<Response> postCompleted = new CompletableFuture<>();
    JsonObject itemMoveWithoutToHoldingsRecordId = new ItemsMoveRequestBuilder(null,
        new JsonArray(Collections.singletonList(UUID.randomUUID()))).create();
    okapiClient.post(ApiRoot.move(), itemMoveWithoutToHoldingsRecordId, ResponseHandler.any(postCompleted));
    Response postResponse1 = postCompleted.get(5, TimeUnit.SECONDS);
    assertThat(postResponse1.getStatusCode(), is(422));
    assertThat(postResponse1.getContentType(), containsString(APPLICATION_JSON));
  }

  @Test
  public void cannotCreateMoveWithoutItemIds()
      throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> postCompleted = new CompletableFuture<>();
    JsonObject itemMoveWithoutItemIds = new ItemsMoveRequestBuilder(UUID.randomUUID(), new JsonArray()).create();
    okapiClient.post(ApiRoot.move(), itemMoveWithoutItemIds, ResponseHandler.any(postCompleted));
    Response postResponse2 = postCompleted.get(5, TimeUnit.SECONDS);
    assertThat(postResponse2.getStatusCode(), is(422));
    assertThat(postResponse2.getContentType(), containsString(APPLICATION_JSON));
  }

  private UUID createHoldingForInstance(UUID instanceId)
      throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    return holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }
}
