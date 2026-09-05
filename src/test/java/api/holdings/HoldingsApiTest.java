package api.holdings;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static support.fixtures.InstanceFixture.smallAngryPlanet;

import api.ApiTestSuite;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.domain.items.EffectiveCallNumberComponents;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;
import support.InstanceApiClient;
import support.builders.HoldingRequestBuilder;
import support.builders.SourceRecordRequestBuilder;
import support.fakes.FakeOkapi;
import support.fixtures.InstanceRequestFixture;

public class HoldingsApiTest extends ApiTests {

  private static final String HOLDINGS_URL = FakeOkapi.getADDRESS() + "/holdings-storage/holdings";

  private static final InventoryConfiguration config = new InventoryConfigurationImpl();

  @Test
  void canUpdateAnExistingHoldings() {
    UUID instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    String adminNote = "This is a note.";
    List<String> administrativeNotes = new ArrayList<>();
    administrativeNotes.add(adminNote);
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withAdministrativeNotes(administrativeNotes))
      .getJson();

    JsonObject updateHoldingsRequest = newHoldings.copy()
      .put("permanentLocationId", "fcd64ce1-6995-48f0-840e-89ffa2288371");

    Response putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.statusCode(), is(NO_CONTENT.code()));

    Response getResponse = holdingsStorageClient.getById(getId(newHoldings));

    assertThat(getResponse.statusCode(), is(OK.code()));

    JsonObject updatedHoldings = getResponse.getJson();

    assertThat(updatedHoldings.containsKey("administrativeNotes"), is(true));

    List<String> retrievedNotes = JsonArrayHelper
      .toListOfStrings(updatedHoldings.getJsonArray("administrativeNotes"));

    assertThat(retrievedNotes, contains(adminNote));

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getString("permanentLocationId"), is("fcd64ce1-6995-48f0-840e-89ffa2288371"));
  }

  @Test
  void canSuppressFromDiscoveryOnUpdateForMarcRecord() {
    var instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withMarcSource())
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createMarcHoldingsSource());
    sourceRecordStorageClient.create(new SourceRecordRequestBuilder(newHoldings.getString("id")));
    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.statusCode(), is(NO_CONTENT.code()));

    var getResponse = holdingsStorageClient.getById(getId(newHoldings));
    var getRecordResponse = sourceRecordStorageClient.getById(UUID.fromString(newHoldings.getString(("id"))));

    assertThat(getResponse.statusCode(), is(OK.code()));
    assertThat(getRecordResponse.statusCode(), is(OK.code()));

    var updatedHoldings = getResponse.getJson();
    var updatedRecord = getRecordResponse.getJson();

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getBoolean("discoverySuppress"), is(Boolean.TRUE));
    assertThat(updatedRecord.getJsonObject("additionalInfo").getBoolean("suppressDiscovery"), is(Boolean.TRUE));
  }

  @Test
  void cannotSuppressFromDiscoveryForSourceOnUpdateForFolioRecord() {
    var instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createFolioHoldingsSource());
    sourceRecordStorageClient.create(new SourceRecordRequestBuilder(newHoldings.getString("id")));
    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.statusCode(), is(NO_CONTENT.code()));

    var getResponse = holdingsStorageClient.getById(getId(newHoldings));
    var getRecordResponse = sourceRecordStorageClient.getById(UUID.fromString(newHoldings.getString(("id"))));

    assertThat(getResponse.statusCode(), is(OK.code()));
    assertThat(getRecordResponse.statusCode(), is(OK.code()));

    var updatedHoldings = getResponse.getJson();
    var updatedRecord = getRecordResponse.getJson();

    assertThat(updatedHoldings.getString("id"), is(newHoldings.getString("id")));
    assertThat(updatedHoldings.getBoolean("discoverySuppress"), is(Boolean.TRUE));
    assertThat(updatedRecord.getJsonObject("additionalInfo").getBoolean("suppressDiscovery"), is(Boolean.FALSE));
  }

  @Test
  void cannotSuppressFromDiscoveryForSourceOnUpdateIfThatDoesNotExist() {
    var instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    var newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instanceId)
        .withMarcSource())
      .getJson();

    holdingsSourceStorageClient.create(new HoldingRequestBuilder().createMarcHoldingsSource());

    var updateHoldingsRequest = newHoldings.copy()
      .put("discoverySuppress", true);

    var putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.statusCode(), is(INTERNAL_SERVER_ERROR.code()));
    assertThat(putResponse.body().contains(NOT_FOUND.codeAsText()), is(true));
  }

  @Test
  void cannotUpdateAnHoldingsThatDoesNotExist() {
    UUID instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    JsonObject updateHoldingsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();

    Response putResponse = updateHoldings(updateHoldingsRequest);

    assertThat(putResponse.statusCode(), is(NOT_FOUND.code()));
    assertThat(putResponse.body(), is("Holdings not found"));
  }

  @Test
  void canUpdateAnExistingMARCHoldingsIfNoChanges() {
    UUID instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId).withMarcSource()).getJson();
    JsonObject holdingsForUpdate = newHoldings.copy();

    Response putResponse = updateHoldings(holdingsForUpdate);
    assertThat(putResponse.statusCode(), is(NO_CONTENT.code()));
    Response getResponse = holdingsStorageClient.getById(getId(newHoldings));

    assertThat(getResponse.statusCode(), is(OK.code()));
    JsonObject updatedHoldings = getResponse.getJson();
    newHoldings.stream()
      .forEach(e -> assertEquals(updatedHoldings.getMap().get(e.getKey()), e.getValue()));
  }

  @Test
  void canNotUpdateAnExistingMARCHoldingsIfBlockedFieldsAreChanged() {
    UUID instanceId = instancesClient.create(InstanceRequestFixture.smallAngryPlanet()).getId();
    JsonObject newHoldings = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId).withMarcSource()).getJson();

    JsonObject holdingsForUpdate = marcHoldingsWithDefaultBlockedFields(getId(newHoldings));

    for (String field : config.getHoldingsBlockedFields()) {
      Response putResponse = updateHoldings(holdingsForUpdate);

      assertThat(putResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
      assertThat(putResponse.getJson().getJsonArray("errors").size(), is(1));

      holdingsForUpdate.remove(field);
    }
  }

  @Test
  void canCreateAHolding() {
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
  void cannotCreateAHoldingWithoutPermanentLocationId() {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    JsonObject holdingAsJson = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(createdInstance.getString("id"))).create();

    holdingAsJson.remove("permanentLocationId");

    assertThat(createHolding(holdingAsJson).statusCode(), is(422));
  }

  @Test
  void cannotCreateAHoldingWithoutInstanceId() {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    JsonObject holdingAsJson = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(createdInstance.getString("id")))
      .permanentlyInMainLibrary().create();

    holdingAsJson.remove("instanceId");

    assertThat(createHolding(holdingAsJson).statusCode(), is(422));
  }

  @Test
  void cannotUpdateAHoldingWithOptimisticLockingFailure() {

    JsonObject instance = createInstance(smallAngryPlanet(UUID.randomUUID()));
    JsonObject holding = new HoldingRequestBuilder()
      .forInstance(UUID.fromString(instance.getString("id")))
      .permanentlyInMainLibrary()
      .create()
      .put("id", ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE);
    assertThat(createHolding(holding).statusCode(), is(201));

    assertThat(updateHolding(holding).statusCode(), is(409));
  }

  @Test
  void canCreateHoldingWithAdditionalCallNumbers() {
    JsonObject instance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    List<EffectiveCallNumberComponents> additionalCallNumbers = new ArrayList<>();
    additionalCallNumbers.add(new EffectiveCallNumberComponents("123", "prefix", "suffix", "typeId"));
    JsonObject holding = new HoldingRequestBuilder().forInstance(UUID.fromString(instance.getString("id")))
      .withAdditionalCallNumbers(additionalCallNumbers).create();
    assertThat(createHolding(holding).statusCode(), is(201));
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

  @SneakyThrows
  private Response updateHoldings(JsonObject holdings) {
    String holdingsUpdateUri = String
      .format("%s/%s", ApiRoot.holdings(), holdings.getString("id"));

    final var putFuture = okapiClient.put(holdingsUpdateUri, holdings);

    return putFuture.toCompletableFuture().get(5, SECONDS);
  }

  private JsonObject createInstance(JsonObject newInstanceRequest) {
    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  @SneakyThrows
  private Response createHolding(JsonObject newHoldingRequest) {
    final var postCompleted = okapiClient.post(new URI(HOLDINGS_URL).toURL(), newHoldingRequest);
    return postCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @SneakyThrows
  private Response updateHolding(JsonObject holding) {
    final var putCompleted = okapiClient.put(new URI(HOLDINGS_URL + "/" + holding.getString("id")).toURL(), holding);
    return putCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }
}
