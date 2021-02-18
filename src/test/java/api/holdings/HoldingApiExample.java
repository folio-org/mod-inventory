package api.holdings;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Test;

import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import io.vertx.core.json.JsonObject;
import support.fakes.FakeOkapi;

public class HoldingApiExample extends ApiTests {

  private static final String HOLDINGS_URL = FakeOkapi.getAddress() + "/holdings-storage/holdings";

  @Test
  public void canCreateAHolding()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    IndividualResource postResponse = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(UUID.fromString(createdInstance.getString("id")))
      .permanentlyInMainLibrary());

    JsonObject createdHolding = holdingsStorageClient.getById(postResponse.getId()).getJson();

    assertTrue(createdHolding.containsKey("id"));

    assertEquals(createdInstance.getString("id"), createdHolding.getString("instanceId"));

    assertTrue(createdHolding.containsKey("permanentLocationId"));
  }

  @Test
  public void cannotCreateAHoldingWithoutPermanentLocationId()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    JsonObject holdingAsJson = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(createdInstance.getString("id"))).create();

    holdingAsJson.remove("permanentLocationId");

    final var postCompleted = okapiClient.post(new URL(HOLDINGS_URL), holdingAsJson);

    Response postResponse = postCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(422));
  }

  @Test
  public void cannotCreateAHoldingWithoutInstanceId()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    JsonObject holdingAsJson = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(createdInstance.getString("id")))
      .permanentlyInMainLibrary().create();

    holdingAsJson.remove("instanceId");

    final var postCompleted = okapiClient.post(new URL(HOLDINGS_URL), holdingAsJson);

    Response postResponse = postCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(422));
  }

  private JsonObject createInstance(JsonObject newInstanceRequest)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }
}
