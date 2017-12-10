package api.items;

import api.ApiTestSuite;
import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.fixtures.InstanceRequestExamples;
import api.support.fixtures.ItemRequestExamples;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.IndividualResource;
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
        .inMainLibrary()
        .create())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .inAnnex() //Deliberately different to demonstrate precedence
        .withNoTemporaryLocation());

    JsonObject createdItem = response.getJson();

    assertThat("has location",
      createdItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getMainLibraryLocation()));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("name"),
      is("Main Library"));
  }

  @Test
  public void permanentLocationIsFromItemWhenNoHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(null)
        .inMainLibrary()
        .withNoTemporaryLocation());

    JsonObject createdItem = response.getJson();

    assertThat("has location",
      createdItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getMainLibraryLocation()));

    assertThat("location is taken from holding",
      createdItem.getJsonObject("permanentLocation").getString("name"),
      is("Main Library"));
  }

  @Test
  public void noPermanentLocationWhenNoHoldingAndNoPermanentLocationOnItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(null)
        .withNoPermanentLocation()
        .withNoTemporaryLocation());

    JsonObject createdItem = response.getJson();

    assertThat("does not have location",
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
        .inMainLibrary()
        .create())
      .getId();

    UUID firstItemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .inAnnex() // deliberately different to demonstrate behaviour
        .temporarilyInAnnex()
        .forHolding(firstHoldingId))
      .getId();

    UUID secondInstanceId = instancesClient.create(
      InstanceRequestExamples.temeraire()).getId();

    UUID secondHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(secondInstanceId)
        .inAnnex()
        .create())
      .getId();

    UUID secondItemId = itemsClient.create(
      ItemRequestExamples.basedUponTemeraire()
        .inMainLibrary() // deliberately different to demonstrate behaviour
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
      is(ApiTestSuite.getMainLibraryLocation()));

    assertThat("location is taken from holding",
      firstFetchedItem.getJsonObject("permanentLocation").getString("name"),
      is("Main Library"));

    JsonObject secondFetchedItem = getRecordById(
      fetchedItemsResponse, secondItemId).get();

    assertThat("has location",
      secondFetchedItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from holding",
      secondFetchedItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getAnnexLocation()));

    assertThat("location is taken from holding",
      secondFetchedItem.getJsonObject("permanentLocation").getString("name"),
      is("Annex Library"));
  }

  @Test
  public void permanentLocationsAreFromItemWhenNoHoldingForItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID itemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .inAnnex() // deliberately different to demonstrate behaviour
        .temporarilyInAnnex()
        .forHolding(null))
      .getId();

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(1));

    JsonObject fetchedItem = getRecordById(
      fetchedItemsResponse, itemId).get();

    assertThat("has location",
      fetchedItem.containsKey("permanentLocation"), is(true));

    assertThat("location is taken from item",
      fetchedItem.getJsonObject("permanentLocation").getString("id"),
      is(ApiTestSuite.getAnnexLocation()));

    assertThat("location is taken from item",
      fetchedItem.getJsonObject("permanentLocation").getString("name"),
      is("Annex Library"));
  }

  @Test
  public void noPermanentLocationsAreFromItemWhenNoHoldingForItemsAndNoPermanentLocationOnItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID itemId = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .withNoPermanentLocation()
        .forHolding(null))
      .getId();

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), is(1));

    JsonObject fetchedItem = getRecordById(
      fetchedItemsResponse, itemId).get();

    assertThat("has location",
      fetchedItem.containsKey("permanentLocation"), is(false));
  }
}
