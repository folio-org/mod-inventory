package api.items;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.support.InstanceSamples.smallAngryPlanet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.AbstractBuilder;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.builders.ItemsMoveRequestBuilder;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import junitparams.JUnitParamsRunner;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class ItemApiMoveExamples extends ApiTests {
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";

  @Test
  public void canMoveItemsToDifferentHoldingsRecord() {
    final var instanceId = createInstance();

    final var existingHoldingsId = createHoldingsForInstance(instanceId);
    final var newHoldingsId = createHoldingsForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withOrder(10)
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingsId, firstItem, secondItem);

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(toList(moveItemsResponse.getJson(), "nonUpdatedIds"), hasSize(0));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    final var firstUpdatedItem = itemsClient.getById(firstItem.getId()).getJson();
    final var secondUpdatedItem = itemsClient.getById(secondItem.getId()).getJson();

    assertThat(firstUpdatedItem.getString(HOLDINGS_RECORD_ID), is(newHoldingsId.toString()));
    assertThat(secondUpdatedItem.getString(HOLDINGS_RECORD_ID), is(newHoldingsId.toString()));

    assertThat(firstUpdatedItem.getInteger("order"), nullValue());
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedItemsCouldNotBeMoved() {
    final var instanceId = createInstance();

    final var existingHoldingId = createHoldingsForInstance(instanceId);
    final var newHoldingsId = createHoldingsForInstance(instanceId);

    final var nonExistentItemId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingsId, nonExistentItemId, item.getId());

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    final var notFoundIds = toListOfStrings(moveItemsResponse.getJson(),
      "nonUpdatedIds");

    assertThat(notFoundIds, hasSize(1));
    assertThat(notFoundIds.get(0), equalTo(nonExistentItemId.toString()));

    final var updatedItem = itemsClient.getById(item.getId()).getJson();

    assertThat(newHoldingsId.toString(), equalTo(updatedItem.getString(HOLDINGS_RECORD_ID)));
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

    final var existingHoldingsId = createHoldingsForInstance(instanceId);
    final var nonExistentHoldingsId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(nonExistentHoldingsId, item);

    assertThat(moveItemsResponse.getStatusCode(), is(422));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(moveItemsResponse.getBody(), containsString("errors"));
    assertThat(moveItemsResponse.getBody(), containsString(nonExistentHoldingsId.toString()));
  }

  @Test
  public void canMoveItemToHoldings() {
    final var instanceId = createInstance();

    final var existingHoldingsId = createHoldingsForInstance(instanceId);
    final var newHoldingsId = createNonCompatibleHoldingForInstance(instanceId);

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingsId, item);

    assertThat(moveItemsResponse.getStatusCode(), is(200));

    final var updatedItem = itemsClient.getById(item.getId());

    assertNotEquals(updatedItem.getJson().getString(HOLDINGS_RECORD_ID), existingHoldingsId.toString());
  }

  @Test
  public void canMoveItemsDueToItemUpdateError() {
    final var instanceId = createInstance();

    final var existingHoldingsId = createHoldingsForInstance(instanceId);
    final var newHoldingsId = createHoldingsForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    // Fake storage module fails updates to item's with the ID_FOR_FAILURE ID
    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607546")
        .withId(ID_FOR_FAILURE)
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var moveItemsResponse = moveItems(newHoldingsId, firstItem, secondItem);

    final var nonUpdatedIdsIds = toListOfStrings(moveItemsResponse.getJson(),
      "nonUpdatedIds");

    assertThat(nonUpdatedIdsIds, hasSize(1));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(moveItemsResponse.getStatusCode(), is(200));
    assertThat(moveItemsResponse.getContentType(), containsString(APPLICATION_JSON));

    final var firstItemUpdated = itemsClient.getById(firstItem.getId()).getJson();

    assertThat(newHoldingsId.toString(), equalTo(firstItemUpdated.getString(HOLDINGS_RECORD_ID)));

    final var secondUpdatedItem = itemsClient.getById(secondItem.getId()).getJson();

    assertThat(existingHoldingsId.toString(), equalTo(secondUpdatedItem.getString(HOLDINGS_RECORD_ID)));
  }

  private Response moveItems(UUID newHoldingsId, IndividualResource... items) {
    final var itemIds = Stream.of(items)
      .map(IndividualResource::getId)
      .toList();

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

  private UUID createHoldingsForInstance(UUID instanceId) {
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
