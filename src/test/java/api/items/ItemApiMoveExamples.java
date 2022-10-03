package api.items;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.support.InstanceSamples.smallAngryPlanet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.AbstractBuilder;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.builders.ItemsMoveRequestBuilder;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import lombok.SneakyThrows;

@RunWith(JUnitParamsRunner.class)
public class ItemApiMoveExamples extends ApiTests {
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

  @Test
  public void canMoveItemsToDifferentHoldingsRecord() {
    final var instanceId = createInstance();

    final var existedHoldingId = createHoldingForInstance(instanceId);
    final var newHoldingId = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingId, firstItem, secondItem);

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(moveItemsResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject updatedItem1 = itemsClient.getById(firstItem.getId())
      .getJson();
    JsonObject updatedItem2 = itemsClient.getById(secondItem.getId())
      .getJson();

    Assert.assertEquals(newHoldingId.toString(), updatedItem1.getString(HOLDINGS_RECORD_ID));
    Assert.assertEquals(newHoldingId.toString(), updatedItem2.getString(HOLDINGS_RECORD_ID));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedItemsCouldNotBeMoved() {
    final var instanceId = createInstance();

    final var existedHoldingId = createHoldingForInstance(instanceId);
    final var newHoldingId = createHoldingForInstance(instanceId);

    final var nonExistedItemId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingId, nonExistedItemId, item.getId());

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    final var notFoundIds = moveItemsResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.get(0), equalTo(nonExistedItemId.toString()));

    JsonObject updatedItem1 = itemsClient.getById(item.getId())
      .getJson();
    assertThat(newHoldingId.toString(), equalTo(updatedItem1.getString(HOLDINGS_RECORD_ID)));
  }

  @Test
  public void cannotMoveItemsToUnspecifiedHoldingsRecord() {
    final var moveItemsResponse = moveItems(null, UUID.randomUUID());

    assertThat(moveItemsResponse.getStatusCode(), is(422));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(moveItemsResponse.getBody(), containsString("errors"));
    assertThat(moveItemsResponse.getBody(), containsString("toHoldingsRecordId"));
    assertThat(moveItemsResponse.getBody(), containsString("toHoldingsRecordId is a required field"));
  }

  @Test
  public void cannotMoveUnspecifiedItems() {
    final var moveItemsResponse = moveItems(UUID.randomUUID(), List.of());

    assertThat(moveItemsResponse.getStatusCode(), is(422));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(moveItemsResponse.getBody(), containsString("errors"));
    assertThat(moveItemsResponse.getBody(), containsString("itemIds"));
    assertThat(moveItemsResponse.getBody(), containsString("Item ids aren't specified"));
  }

  @Test
  public void cannotMoveToNonExistedHoldingsRecord() {
    final var instanceId = createInstance();

    final var existedHoldingId = createHoldingForInstance(instanceId);
    final var newHoldingId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingId, item);

    assertThat(moveItemsResponse.getStatusCode(), is(422));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(moveItemsResponse.getBody(), containsString("errors"));
    assertThat(moveItemsResponse.getBody(), containsString(newHoldingId.toString()));
  }

  @Test
  public void canMoveToHoldingsRecordWithHoldingSchemasMismatching() {
    final var instanceId = createInstance();

    final var existedHoldingId = createHoldingForInstance(instanceId);
    final var newHoldingId = createNonCompatibleHoldingForInstance(instanceId);

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingId, item);

    assertThat(moveItemsResponse.getStatusCode(), is(200));
  }

  @Test
  public void canMoveItemsDueToItemUpdateError() {
    final var instanceId = createInstance();

    final var existedHoldingId = createHoldingForInstance(instanceId);
    final var newHoldingId = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existedHoldingId)
        .withBarcode("645398607546")
        .withId(ID_FOR_FAILURE)
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingId, firstItem, secondItem);

    final var nonUpdatedIdsIds = moveItemsResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(1));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject updatedItem1 = itemsClient.getById(firstItem.getId())
      .getJson();
    assertThat(newHoldingId.toString(), equalTo(updatedItem1.getString(HOLDINGS_RECORD_ID)));

    JsonObject updatedItem2 = itemsClient.getById(secondItem.getId())
      .getJson();
    assertThat(existedHoldingId.toString(), equalTo(updatedItem2.getString(HOLDINGS_RECORD_ID)));
  }

  private Response moveItems(UUID newHoldingsId, IndividualResource... items) {
    final var itemIds = Stream.of(items)
      .map(IndividualResource::getId)
      .collect(Collectors.toList());

    return moveItems(newHoldingsId, itemIds);
  }

  private Response moveItems(UUID newHoldingsId, UUID... itemIds) {
    return moveItems(newHoldingsId, Arrays.asList(itemIds));
  }

  private Response moveItems(UUID newHoldingsId, List<UUID> itemIds) {
    return moveItems(new ItemsMoveRequestBuilder(newHoldingsId, itemIds).create());
  }

  @SneakyThrows
  private Response moveItems(JsonObject body) {
    return okapiClient.post(ApiRoot.moveItems(), body)
      .toCompletableFuture().get(5, SECONDS);
  }

  private static UUID createInstance() {
    final var instanceId = UUID.randomUUID();

    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(instanceId));

    return instanceId;
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId))
      .getId();
  }

  private UUID createNonCompatibleHoldingForInstance(UUID instanceId) {
    final var builder = new InvalidHoldingRequestBuilder()
      .forInstance(instanceId);

    return holdingsStorageClient.create(builder).getId();
  }
  private static class InvalidHoldingRequestBuilder extends AbstractBuilder {
    private final UUID instanceId;
    private final UUID permanentLocationId;

    InvalidHoldingRequestBuilder() {
      this(null, UUID.fromString(ApiTestSuite.getThirdFloorLocation()));
    }

    InvalidHoldingRequestBuilder(
      UUID instanceId,
      UUID permanentLocationId) {
      this.instanceId = instanceId;
      this.permanentLocationId = permanentLocationId;
    }

    public InvalidHoldingRequestBuilder forInstance(UUID instanceId) {
      return new InvalidHoldingRequestBuilder(
        instanceId,
        this.permanentLocationId);
    }

    @Override
    public JsonObject create() {
      JsonObject holding = new JsonObject();

      holding.put("instanceId", instanceId.toString())
        .put("permanentLocationId", permanentLocationId.toString());

      holding.put("unspecified", "unspecified");

      return holding;
    }
  }
}
