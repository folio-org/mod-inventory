package api.items;

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
public class ItemApiTitleExamples extends ApiTests {
  public ItemApiTitleExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void titleIsBasedUponInstance()
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
        .create())
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withTitle("A different item title")); //Deliberately different to demonstrate precedence

    JsonObject createdItem = response.getJson();

    assertThat("has title from instance",
      createdItem.getString("title"), is("The Long Way to a Small, Angry Planet"));
  }

  @Test
  public void titleIsFromItemWhenNoInstance()
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
        .create())
      .getId();

    instancesClient.delete(instanceId);

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(holdingId)
        .withTitle("A different item title")); //Deliberately different to demonstrate precedence

    JsonObject createdItem = response.getJson();

    assertThat("has title from item",
      createdItem.getString("title"), is("A different item title"));
  }

  @Test
  public void titleIsFromItemWhenNoHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(null)
        .withTitle("A different item title"));

    JsonObject createdItem = response.getJson();

    assertThat("has title from item",
      createdItem.getString("title"), is("A different item title"));
  }

  @Test
  public void noTitleWhenNoHoldingAndNoTitleOnItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    IndividualResource response = itemsClient.create(
      ItemRequestExamples.basedUponSmallAngryPlanet()
        .forHolding(null)
        .withNoTitle());

    JsonObject createdItem = response.getJson();

    assertThat("has no title",
      createdItem.containsKey("title"), is(false));
  }
}
