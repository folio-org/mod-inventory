package api.items;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.http.BusinessLogicInterfaceUrls;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class ItemMarkWithdrawnApiTest extends ApiTests {

  @Parameters({
    "In process",
    "Available",
    "In transit",
    "Awaiting pickup",
    "Awaiting delivery",
    "Missing",
    "Paged"
  })
  @Test
  public void canWithdrawItemWhenInAllowedStatus(String initialStatus) throws Exception {
    IndividualResource instance = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instance.getId()))
      .getId();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus(initialStatus)
      .canCirculate());

    final JsonObject withdrawnItem = markItemWithdrawn(createdItem.getId()).getJson();
    assertThat(withdrawnItem.getJsonObject("status").getString("name"),
      is("Withdrawn"));

    assertThat(itemsClient.getById(createdItem.getId()).getJson()
      .getJsonObject("status").getString("name"), is("Withdrawn"));
  }

  @Parameters({
    "On order",
    "Checked out",
    "Withdrawn",
    "Claimed returned",
    "Declared lost"
  })
  @Test
  public void cannotWithdrawIItemWhenNotInAllowedStatus(String initialStatus) throws Exception {
    IndividualResource instance = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instance.getId()))
      .getId();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus(initialStatus)
      .canCirculate());

    final Response response = markItemWithdrawn(createdItem.getId());

    assertThat(response, hasValidationError("Item is not allowed to be marked as Withdrawn",
      "status.name", initialStatus));
  }

  @Test
  public void shouldWithdrawItemThatCannotBeFound() throws Exception {
    assertThat(markItemWithdrawn(UUID.randomUUID()).getStatusCode(),
      is(404));
  }

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldChangeRequestBeingFulfilledBackToNotYetFilled(String requestStatus) throws Exception {
    IndividualResource instance = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instance.getId()))
      .getId();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    final JsonObject withdrawnItem = markItemWithdrawn(createdItem.getId()).getJson();
    assertThat(withdrawnItem.getJsonObject("status").getString("name"),
      is("Withdrawn"));

    assertThat(itemsClient.getById(createdItem.getId()).getJson()
      .getJsonObject("status").getString("name"), is("Withdrawn"));

    assertThat(requestStorageClient.getById(request.getId()).getJson()
      .getString("status"), is("Open - Not yet filled"));
  }

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldNotReopenExpiredRequests(String requestStatus) throws Exception {
    IndividualResource instance = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instance.getId()))
      .getId();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).minusHours(1));

    final JsonObject withdrawnItem = markItemWithdrawn(createdItem.getId()).getJson();
    assertThat(withdrawnItem.getJsonObject("status").getString("name"),
      is("Withdrawn"));

    assertThat(itemsClient.getById(createdItem.getId()).getJson()
      .getJsonObject("status").getString("name"), is("Withdrawn"));

    assertThat(requestStorageClient.getById(request.getId()).getJson()
      .getString("status"), is(requestStatus));
  }

  @Test
  @Parameters({
    "Closed - Cancelled",
    "Closed - Filled",
    "Closed - Pickup expired",
    "Closed - Unfilled"
  })
  public void shouldNotReopenClosedRequests(String requestStatus) throws Exception {
    IndividualResource instance = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(instance.getId()))
      .getId();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus("Awaiting pickup")
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    final JsonObject withdrawnItem = markItemWithdrawn(createdItem.getId()).getJson();
    assertThat(withdrawnItem.getJsonObject("status").getString("name"),
      is("Withdrawn"));

    assertThat(itemsClient.getById(createdItem.getId()).getJson()
      .getJsonObject("status").getString("name"), is("Withdrawn"));

    assertThat(requestStorageClient.getById(request.getId()).getJson()
      .getString("status"), is(requestStatus));
  }

  private Response markItemWithdrawn(UUID itemId) throws InterruptedException,
    ExecutionException, TimeoutException {

    final CompletableFuture<Response> future = new CompletableFuture<>();

    okapiClient.post(BusinessLogicInterfaceUrls.markWithdrawn(itemId.toString()),
      null, ResponseHandler.any(future));

    return future.get(5, TimeUnit.SECONDS);
  }

  private IndividualResource createRequest(UUID itemId,
    String status, DateTime expireDateTime)
    throws InterruptedException, ExecutionException, TimeoutException, MalformedURLException {

    final JsonObject request = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", status)
      .put("itemId", itemId.toString())
      .put("holdShelfExpirationDate", expireDateTime.toString())
      .put("requesterId", UUID.randomUUID().toString())
      .put("requestType", "Hold")
      .put("requestDate", DateTime.now(DateTimeZone.UTC).toString())
      .put("fulfilmentPreference", "Hold shelf");

    return requestStorageClient.create(request);
  }
}
