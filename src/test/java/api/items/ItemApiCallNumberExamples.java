package api.items;

import static api.support.JsonCollectionAssistant.getRecordById;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Ignore;
import org.junit.Test;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.fixtures.InstanceRequestExamples;
import api.support.fixtures.ItemRequestExamples;
import io.vertx.core.json.JsonObject;

//TODO: When converted to RAML module builder, no longer redirect to content and do separate GET
public class ItemApiCallNumberExamples extends ApiTests {
  public ItemApiCallNumberExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void callNumberIsIncludedFromHoldingsRecord()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withCallNumber("R11.A38"))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId));

    JsonObject createdItem = response.getJson();

    assertThat("has call number from holdings record",
      createdItem.getString("callNumber"), is("R11.A38"));
  }

  @Ignore("mod-inventory-storage disallows this scenario, change to be isolated test")
  @Test
  public void noCallNumberWhenNoHoldingsRecord()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withCallNumber("R11.A38"))
      .getId();

    holdingsStorageClient.delete(holdingId);

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId));

    JsonObject createdItem = response.getJson();

    assertThat("has no call number",
      createdItem.containsKey("callNumber"), is(false));
  }

  @Test
  public void callNumbersComeFromHoldingsForMultipleItems()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    UUID firstInstanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID firstHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(firstInstanceId)
        .withCallNumber("R11.A38"))
      .getId();

    UUID firstItemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(firstHoldingId))
        .getId();

    UUID secondInstanceId = instancesClient.create(
      InstanceRequestExamples.temeraire()).getId();

    UUID secondHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(secondInstanceId)
        .withCallNumber("D15.H63 A3 2002"))
      .getId();

    UUID secondItemId = itemsClient.create(
      ItemRequestExamples.basedUponTemeraire()
        .forHolding(secondHoldingId))
      .getId();

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(2));

    JsonObject firstFetchedItem = getRecordById(
      fetchedItemsResponse, firstItemId).orElse(new JsonObject());

    assertThat("has call number from holdings record",
      firstFetchedItem.getString("callNumber"), is("R11.A38"));

    JsonObject secondFetchedItem = getRecordById(
      fetchedItemsResponse, secondItemId).orElse(new JsonObject());

    assertThat("has call number from holdings record",
      secondFetchedItem.getString("callNumber"), is("D15.H63 A3 2002"));
  }

  @Ignore("mod-inventory-storage disallows this scenario, change to be isolated test")
  @Test
  public void noCallNumberWhenNoHoldingForMultipleItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.temeraire()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withCallNumber(""))
      .getId();

    UUID itemId = itemsClient.create(
      ItemRequestExamples.basedUponTemeraire()
        .forHolding(holdingId))
      .getId();

    holdingsStorageClient.delete(holdingId);

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(1));

    JsonObject fetchedItem = getRecordById(
      fetchedItemsResponse, itemId).orElse(new JsonObject());

    assertThat("has no call number",
      fetchedItem.containsKey("callNumber"), is(false));
  }

  @Test
  public void readOnlyCallNumberIsNotStoredWhenCreated()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withCallNumber("foo"))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .withReadOnlyCallNumber("Should be discarded")
        .forHolding(holdingId));

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("call number should not be stored",
      storedItemResponse.getJson().containsKey("callNumber"), is(false));
  }

  @Test
  public void readOnlyCallNumberIsNotStoredWhenUpdated()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .withReadOnlyCallNumber("Should be discarded")
        .forHolding(holdingId));

    itemsClient.replace(response.getId(), response.getJson());

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("call number should not be stored",
      storedItemResponse.getJson().containsKey("callNumber"), is(false));
  }
}
