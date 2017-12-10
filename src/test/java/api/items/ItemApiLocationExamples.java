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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
}
