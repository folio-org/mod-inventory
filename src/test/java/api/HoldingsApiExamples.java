package api;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.fixtures.InstanceRequestExamples;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Test;

import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.http.client.Response;

public class HoldingsApiExamples extends ApiTests {

  private static final InventoryConfiguration config = new InventoryConfigurationImpl();

  @After
  public void disableFailureEmulation() throws Exception {
    holdingsStorageClient.disableFailureEmulation();
  }

  @Test
  public void canUpdateAnExistingHoldings() throws Exception {
    UUID instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();

    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId)).getJson();

    JsonObject updateHoldingsRequest = newHoldings.copy()
      .put("permanentLocationId", "fcd64ce1-6995-48f0-840e-89ffa2288371");

    Response putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(NO_CONTENT.code()));

    Response getResponse = holdingsStorageClient.getById(getId(newHoldings));

    assertThat(getResponse.getStatusCode(), is(OK.code()));

    JsonObject updatedHoldings = getResponse.getJson();

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getString("permanentLocationId"), is("fcd64ce1-6995-48f0-840e-89ffa2288371"));
  }

  @Test
  public void cannotUpdateAnHoldingsThatDoesNotExist() throws Exception {
    UUID instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    JsonObject updateHoldingsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();

    Response putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(NOT_FOUND.code()));
    assertThat(putResponse.getBody(), is("Holdings not found"));
  }

  @Test
  public void canUpdateAnExistingMARCHoldingsIfNoChanges() throws Exception {
    UUID instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId).withMarcSource()).getJson();
    JsonObject holdingsForUpdate = newHoldings.copy();

    Response putResponse = updateHoldings(holdingsForUpdate);
    assertThat(putResponse.getStatusCode(), is(NO_CONTENT.code()));
    Response getResponse = holdingsStorageClient.getById(getId(newHoldings));

    assertThat(getResponse.getStatusCode(), is(OK.code()));
    JsonObject updatedHoldings = getResponse.getJson();
    newHoldings.stream()
      .forEach(e -> assertEquals(updatedHoldings.getMap().get(e.getKey()), e.getValue()));
  }

  @Test
  public void canNotUpdateAnExistingMARCHoldingsIfBlockedFieldsAreChanged() throws Exception {
    UUID instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId).withMarcSource()).getJson();

    JsonObject holdingsForUpdate = marcHoldingsWithDefaultBlockedFields(getId(newHoldings));

    for (String field : config.getHoldingsBlockedFields()) {
      Response putResponse = updateHoldings(holdingsForUpdate);

      assertThat(putResponse.getStatusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
      assertThat(putResponse.getJson().getJsonArray("errors").size(), is(1));

      holdingsForUpdate.remove(field);
    }
  }

  private UUID getId(JsonObject newHoldings) {
    return UUID.fromString(newHoldings.getString("id"));
  }

  private JsonObject marcHoldingsWithDefaultBlockedFields(UUID id) {
    return new JsonObject()
      .put("id", id.toString())
      // blocked fields
      .put("hrid", UUID.randomUUID().toString())
      .put("callNumberPrefix", "callNumberPrefix")
      .put("callNumberSuffix", "callNumberSuffix")
      .put("callNumberTypeId", "callNumberTypeId")
      .put("callNumber", "callNumber")
      .put("copyNumber", "copyNumber")
      .put("shelvingTitle", "shelvingTitle")
      .put("holdingsTypeId", "03c9c400-b9e3-4a07-ac0e-05ab470233ed")
      .put("permanentLocationId", "03c9c400-b9e3-4a07-ac0e-05ab470233ed")
      .put("notes", new JsonArray()
        .add(JsonObject.mapFrom(new JsonObject().put("note", "test note").put("staffOnly", false))))
      .put("holdingsStatements", new JsonArray().add(new JsonObject().put("statement", "test series")))
      .put("holdingsStatementsForSupplements", new JsonArray().add(new JsonObject().put("statement", "test series")))
      .put("holdingsStatementsForIndexes", new JsonArray().add(new JsonObject().put("statement", "test series")));
  }

  private Response updateHoldings(JsonObject holdings) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    String holdingsUpdateUri = String
      .format("%s/%s", ApiRoot.holdings(), holdings.getString("id"));

    final var putFuture = okapiClient.put(holdingsUpdateUri, holdings);

    return putFuture.toCompletableFuture().get(5, SECONDS);
  }
}
