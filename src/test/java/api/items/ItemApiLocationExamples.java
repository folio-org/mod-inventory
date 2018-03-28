package api.items;

import api.ApiTestSuite;
import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.fixtures.InstanceRequestExamples;
import api.support.fixtures.ItemRequestExamples;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static api.support.JsonCollectionAssistant.getRecordById;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

//TODO: When converted to RAML module builder, no longer redirect to content and do separate GET
public class ItemApiLocationExamples extends ApiTests {
  public ItemApiLocationExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void locationIsBasedUponHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId)
        .inMainLibrary())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withNoTemporaryLocation());

    JsonObject createdItem = response.getJson();

    assertThat("has location",
      createdItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getThirdFloorLocation()));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("name"),
      is("3rd Floor"));
  }

  @Test
  public void noPermanentLocationWhenNoHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId))
      .getId();

    holdingsStorageClient.delete(holdingId);

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withNoTemporaryLocation());

    JsonObject createdItem = response.getJson();

    assertThat("does not have permanent location",
      createdItem.containsKey("permanentLocation"), is(false));
  }

  @Test
  public void permanentLocationsComeFromHoldingsForMultipleItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID firstInstanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID firstHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(firstInstanceId)
        .inMainLibrary())
      .getId();

    UUID firstItemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .temporarilyInAnnex()
        .forHolding(firstHoldingId))
      .getId();

    UUID secondInstanceId = instancesClient.create(
      InstanceRequestExamples.temeraire()).getId();

    UUID secondHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(secondInstanceId)
        .inAnnex())
      .getId();

    UUID secondItemId = itemsClient.create(
      ItemRequestExamples.basedUponTemeraire()
        .temporarilyInMainLibrary()
        .forHolding(secondHoldingId))
      .getId();

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(2));

    JsonObject firstFetchedItem = getRecordById(
      fetchedItemsResponse, firstItemId).get();

    assertThat("has location",
      firstFetchedItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      firstFetchedItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getThirdFloorLocation()));

    assertThat("location is taken from holding",
      firstFetchedItem.getJsonObject("permanentLocation").getString("name"),
      is("3rd Floor"));

    JsonObject secondFetchedItem = getRecordById(
      fetchedItemsResponse, secondItemId).get();

    assertThat("has location",
      secondFetchedItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      secondFetchedItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getMezzanineDisplayCaseLocation()));

    assertThat("location is taken from holding",
      secondFetchedItem.getJsonObject("permanentLocation").getString("name"),
      is("Display Case, Mezzanine"));
  }

  @Test
  public void noPermanentLocationWhenNoHoldingForItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId))
      .getId();

    UUID itemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .temporarilyInAnnex()
        .forHolding(holdingId))
      .getId();

    holdingsStorageClient.delete(holdingId);

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(1));

    JsonObject fetchedItem = getRecordById(
      fetchedItemsResponse, itemId).get();

    assertThat("has no permanent location",
      fetchedItem.containsKey("permanentLocation"), is(false));
  }

  @Test
  public void readOnlyPermanentLocationIsNotStoredWhenCreated()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .withReadOnlyPermanentLocation(ItemRequestBuilder.annex())
        .forHolding(holdingId));

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("permanent location should not be stored",
      storedItemResponse.getJson().containsKey("permanentLocationId"), is(false));
  }

  @Test
  public void readOnlyPermanentLocationIsNotStoredWhenUpdated()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID instanceId = instancesClient.create(
      InstanceRequestExamples.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .withReadOnlyPermanentLocation(ItemRequestBuilder.annex())
        .forHolding(holdingId));

    itemsClient.replace(response.getId(), response.getJson());

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("permanent location should not be stored",
      storedItemResponse.getJson().containsKey("permanentLocationId"), is(false));
  }
}
