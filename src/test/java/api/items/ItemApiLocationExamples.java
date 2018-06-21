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
  public void effectiveLocationIsPermanentLocationFromHolding()
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
        .permanentlyInMainLibrary()
        .withNoTemporaryLocation())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withNoTemporaryLocation()
        .withNoPermanentLocation());

    JsonObject createdItem = response.getJson();

    assertThat("has effective location",
      createdItem.containsKey("effectiveLocation"), is(true));

    assertThat("effective location is permanent location from holding",
      createdItem.getJsonObject("effectiveLocation").getString("id"),
      is(ApiTestSuite.getMainLibraryLocation()));
  }
  
  @Test
  public void effectiveLocationIsTemporaryLocationFromHolding()
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
        .permanentlyInMainLibrary()
        .temporarilyInMezzanine())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withNoTemporaryLocation()
        .withNoPermanentLocation());

    JsonObject createdItem = response.getJson();

    assertThat("has effective location",
      createdItem.containsKey("effectiveLocation"), is(true));

    assertThat("effective location is temporary location from holding",
      createdItem.getJsonObject("effectiveLocation").getString("id"),
      is(ApiTestSuite.getMezzanineDisplayCaseLocation()));

  }

  @Test
  public void effectiveLocationIsPermanentLocationFromItem()
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
        .permanentlyInMainLibrary()
        .temporarilyInMezzanine())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withNoTemporaryLocation()
        .permanentlyInThirdFloor());

    JsonObject createdItem = response.getJson();

    assertThat("has effective location",
      createdItem.containsKey("effectiveLocation"), is(true));

    assertThat("effective location is permanent location from item",
      createdItem.getJsonObject("effectiveLocation").getString("id"),
      is(ApiTestSuite.getThirdFloorLocation()));
  }

  @Test
  public void effectiveLocationIsTemporaryLocationFromItem()
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
        .permanentlyInMainLibrary()
        .temporarilyInMezzanine())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .temporarilyInReadingRoom()
        .permanentlyInThirdFloor());

    JsonObject createdItem = response.getJson();

    assertThat("has effective location",
      createdItem.containsKey("effectiveLocation"), is(true));

    assertThat("effective location is temporary location from item",
      createdItem.getJsonObject("effectiveLocation").getString("id"),
      is(ApiTestSuite.getReadingRoomLocation()));
  }
  
  //@Test
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
        .permanentlyInMainLibrary())
      .getId();

    UUID firstItemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .temporarilyInReadingRoom()
        .forHolding(firstHoldingId))
      .getId();

    UUID secondInstanceId = instancesClient.create(
      InstanceRequestExamples.temeraire()).getId();

    UUID secondHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(secondInstanceId)
        .permanentlyInMainLibrary())
      .getId();

    UUID secondItemId = itemsClient.create(
      ItemRequestExamples.basedUponTemeraire()
        .temporarilyInReadingRoom()
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
  public void readOnlyEffectiveLocationIsNotStoredWhenCreated()
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
        .withReadOnlyEffectiveLocation(ItemRequestBuilder.readingRoom())
        .forHolding(holdingId));

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("effective location should not be stored",
      storedItemResponse.getJson().containsKey("effectiveLocationId"), is(false));
  }

  @Test
  public void readOnlyEffectiveLocationIsNotStoredWhenUpdated()
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
        .withReadOnlyEffectiveLocation(ItemRequestBuilder.readingRoom())
        .forHolding(holdingId));

    itemsClient.replace(response.getId(), response.getJson());

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("effective location should not be stored",
      storedItemResponse.getJson().containsKey("effectiveLocationId"), is(false));
  }
}
