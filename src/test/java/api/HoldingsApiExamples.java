package api;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.Matchers.contains;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.SourceRecordRequestBuilder;
import api.support.fixtures.InstanceRequestExamples;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Test;

import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.JsonArrayHelper;
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
    String adminNote = "This is a note.";
    List<String> administrativeNotes = new ArrayList<String>();
    administrativeNotes.add(adminNote);
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId)
      .withAdministrativeNotes(administrativeNotes))
      .getJson();

    JsonObject updateHoldingsRequest = newHoldings.copy()
      .put("permanentLocationId", "fcd64ce1-6995-48f0-840e-89ffa2288371");

    Response putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(NO_CONTENT.code()));

    Response getResponse = holdingsStorageClient.getById(getId(newHoldings));

    assertThat(getResponse.getStatusCode(), is(OK.code()));

    JsonObject updatedHoldings = getResponse.getJson();

    assertThat(updatedHoldings.containsKey("administrativeNotes"), is(true));

    List<String> retrievedNotes = JsonArrayHelper
      .toListOfStrings(updatedHoldings.getJsonArray("administrativeNotes"));

    assertThat(retrievedNotes, contains(adminNote));

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getString("permanentLocationId"), is("fcd64ce1-6995-48f0-840e-89ffa2288371"));
  }

  @Test
  public void canSuppressFromDiscoveryOnUpdateForMarcRecord() throws Exception {
    var instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withMarcSource())
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createMarcHoldingsSource());
    sourceRecordStorageClient.create(new SourceRecordRequestBuilder(newHoldings.getString("id")));
    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(NO_CONTENT.code()));

    var getResponse = holdingsStorageClient.getById(getId(newHoldings));
    var getRecordResponse = sourceRecordStorageClient.getById(UUID.fromString(newHoldings.getString(("id"))));

    assertThat(getResponse.getStatusCode(), is(OK.code()));
    assertThat(getRecordResponse.getStatusCode(), is(OK.code()));

    var updatedHoldings = getResponse.getJson();
    var updatedRecord = getRecordResponse.getJson();

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getBoolean("discoverySuppress"), is(Boolean.TRUE));
    assertThat(updatedRecord.getJsonObject("additionalInfo").getBoolean("suppressDiscovery"), is(Boolean.TRUE));
  }

  @Test
  public void cannotSuppressFromDiscoveryForSourceOnUpdateForFolioRecord() throws Exception {
    var instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createFolioHoldingsSource());
    sourceRecordStorageClient.create(new SourceRecordRequestBuilder(newHoldings.getString("id")));
    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(NO_CONTENT.code()));

    var getResponse = holdingsStorageClient.getById(getId(newHoldings));
    var getRecordResponse = sourceRecordStorageClient.getById(UUID.fromString(newHoldings.getString(("id"))));

    assertThat(getResponse.getStatusCode(), is(OK.code()));
    assertThat(getRecordResponse.getStatusCode(), is(OK.code()));

    var updatedHoldings = getResponse.getJson();
    var updatedRecord = getRecordResponse.getJson();

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getBoolean("discoverySuppress"), is(Boolean.TRUE));
    assertThat(updatedRecord.getJsonObject("additionalInfo").getBoolean("suppressDiscovery"), is(Boolean.FALSE));
  }

  @Test
  public void cannotSuppressFromDiscoveryForSourceOnUpdateIfThatDoesNotExist() throws Exception {
    var instanceId = instancesClient.create(InstanceRequestExamples.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withMarcSource())
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createMarcHoldingsSource());

    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.getStatusCode(), is(INTERNAL_SERVER_ERROR.code()));
    assertThat(putResponse.getBody().contains(NOT_FOUND.codeAsText()),is(true));
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
