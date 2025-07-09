package api.holdings;

import api.ApiTestSuite;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.folio.inventory.domain.items.EffectiveCallNumberComponents;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;
import support.fakes.FakeOkapi;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

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

    assertThat(createHolding(holdingAsJson).getStatusCode(), is(422));
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

    assertThat(createHolding(holdingAsJson).getStatusCode(), is(422));
  }

  @Test
  public void cannotUpdateAHoldingWithOptimisticLockingFailure() throws Exception {

    JsonObject instance = createInstance(smallAngryPlanet(UUID.randomUUID()));
    JsonObject holding = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(instance.getString("id")))
      .permanentlyInMainLibrary()
      .create()
      .put("id", ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE);
    assertThat(createHolding(holding).getStatusCode(), is(201));

    assertThat(updateHolding(holding).getStatusCode(), is(409));
  }

  @Test
  public void canCreateHoldingWithAdditionalCallNumbers() throws Exception {
    JsonObject instance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    List<EffectiveCallNumberComponents> additionalCallNumbers = new ArrayList<>();
    additionalCallNumbers.add(new EffectiveCallNumberComponents("123", "prefix", "suffix", "typeId"));
    JsonObject holding = new HoldingRequestBuilder().forInstance(UUID.fromString(instance.getString("id")))
      .withAdditionalCallNumbers(additionalCallNumbers).create();
    assertThat(createHolding(holding).getStatusCode(), is(201));
  }

  private JsonObject createInstance(JsonObject newInstanceRequest)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  private Response createHolding(JsonObject newHoldingRequest)
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    final var postCompleted = okapiClient.post(new URL(HOLDINGS_URL), newHoldingRequest);
    return postCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private Response updateHolding(JsonObject holding)
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    final var putCompleted = okapiClient.put(new URL(HOLDINGS_URL + "/" + holding.getString("id")), holding);
    return putCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }
}
