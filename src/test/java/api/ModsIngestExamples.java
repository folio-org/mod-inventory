package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.awaitility.Duration;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static api.ApiTestSuite.storageOkapiUrl;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class ModsIngestExamples extends ApiTests {
  public ModsIngestExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void canIngestMODSRecords()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    File modsFile = loadFileFromResource(
      "mods/multiple-example-mods-records.xml");

    String statusLocation = given()
      .header("X-Okapi-Url", storageOkapiUrl())
      .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
      .header("X-Okapi-Token", ApiTestSuite.TOKEN)
      .multiPart("record", modsFile)
      .when().post(getIngestUrl())
      .then()
      .log().all()
      .statusCode(202)
      .extract().header("location");

    await()
      .pollDelay(new Duration(1, TimeUnit.SECONDS))
      .atMost(new Duration(10, TimeUnit.SECONDS))
      .catchUncaughtExceptions()
      .untilAsserted(() -> ingestJobHasCompleted(statusLocation));

    List<JsonObject> instances = instancesClient.getAll();

    //TODO: Use business logic interface when built
    //likely after RAML module builder upgrade
    List<JsonObject> holdings = holdingsStorageClient.getAll();

    List<JsonObject> items = itemsClient.getAll();

    assertThat("Should have right number of instances", instances.size(), is(9));
    assertThat("Should have right number of holdings", holdings.size(), is(9));
    assertThat("Should have right number of items", items.size(), is(9));

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "California: its gold and its inhabitants",
      "Huntley, Henry Veel", "69228882");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Studien zur Geschichte der Notenschrift.",
      "Riemann, Karl Wilhelm J. Hugo.", "69247446");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Essays on C.S. Lewis and George MacDonald",
      "Marshall, Cynthia.", "53556908");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Statistical sketches of Upper Canada",
      "Dunlop, William", "69077747");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Edward McGuire, RHA", "Fallon, Brian.", "22169083");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Influenza della Poesia sui Costumi",
      "MABIL, Pier Luigi.", "43620390");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Pavle Nik", "Božović, Ratko.", "37696876");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Grammaire", "Riemann, Othon.", "69250051");

    expectedRelatedHoldingAndItem(instances, holdings, items,
      "Angry Planet", "Unknown contributor", "67437645");

    instancesHaveExpectedProperties(instances);
    holdingsHaveExpectedProperties(holdings);
    itemsHaveExpectedProperties(items);
    itemsHaveExpectedDerivedProperties(items);
    storedItemsDoNotHaveDerivedProperties();
  }

  @Test
  public void willRefuseIngestForMultipleFiles()
    throws MalformedURLException {

    File modsFile = loadFileFromResource("mods/multiple-example-mods-records.xml");

    given()
      .header("X-Okapi-Url", storageOkapiUrl())
      .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
      .header("X-Okapi-Token", ApiTestSuite.TOKEN)
      .multiPart("record", modsFile)
      .multiPart("record", modsFile)
      .when().post(getIngestUrl())
      .then()
      .statusCode(400)
      .body(is("Cannot parse multiple files in a single request"));
  }

  private void expectedRelatedHoldingAndItem(
    List<JsonObject> instances,
    List<JsonObject> holdings,
    List<JsonObject> items, String title,
    String contributor,
    String barcode) {

    Optional<JsonObject> possibleInstance = instances.stream()
      .filter(instance -> InstanceEntitled(instance, title))
      .findFirst();

    assertThat(possibleInstance.isPresent(), is(true));

    JsonObject instance = possibleInstance.get();

    assertThat("instance has contributor",
      hasContributor(instance, contributor), is(true));

    String instanceId = instance.getString("id");
    String instanceTitle = instance.getString("title");

    Optional<JsonObject> possibleHolding = findFirst(holdings,
      holdingForInstance(instanceId));

    assertThat(String.format("Instance %s: %s should have a holding (holdings: %s)",
      instanceId, instanceTitle, holdings),
      possibleHolding.isPresent(), is(true));

    String holdingId = possibleHolding.get().getString("id");

    Optional<JsonObject> possibleItem = findFirst(items, itemForHolding(holdingId));

    assertThat(String.format("Holding %s for %s should have an item (items: %s)",
      holdingId, instanceTitle, items),
      possibleItem.isPresent(), is(true));

    assertThat("item should have correct barcode",
      possibleItem.get().getString("barcode"), is(barcode));
  }

  private boolean hasContributor(
    JsonObject instance,
    String expectedContributorName) {

    if(Objects.isNull(instance) || !instance.containsKey("contributors")) {
      return false;
    }

    return JsonArrayHelper.toList(instance.getJsonArray("contributors")).stream()
      .anyMatch(contributor -> contributor.getString("name").contains(expectedContributorName));
  }

  private Optional<JsonObject> findFirst(
    Collection<JsonObject> collection,
    Predicate<JsonObject> predicate) {

    return collection.stream()
      .filter(predicate)
      .findFirst();
  }

  private Predicate<JsonObject> holdingForInstance(String instanceId) {
    return holding -> StringUtils.equals(holding.getString("instanceId"), instanceId);
  }

  private Predicate<JsonObject> itemForHolding(String holdingId) {
    return item -> StringUtils.equals(item.getString("holdingsRecordId"), holdingId);
  }

  private void ingestJobHasCompleted(String statusLocation)
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(statusLocation, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Should be able to get ingest job status",
      getResponse.getStatusCode(), is(200));

    assertThat("Ingest status should be completed",
      getResponse.getJson().getString("status"), is("Completed"));
  }

  private void instancesHaveExpectedProperties(List<JsonObject> instances) {
    instances.stream().forEach(instance -> {
      assertThat("instance identifier should have an ID",
        instance.containsKey("id"), is(true));

      assertThat("instance should have a title",
        instance.containsKey("title"), is(true));

      assertThat("instance should have a type",
        instance.containsKey("instanceTypeId"), is(true));

      assertThat("instance type should be books",
        instance.getString("instanceTypeId"),
        is(ApiTestSuite.getBooksInstanceType()));

      assertThat("instance should have a source",
        instance.containsKey("source"), is(true));

      assertThat("instance should have identifiers",
        instance.containsKey("identifiers"), is(true));

      List<JsonObject> identifiers = JsonArrayHelper.toList(
        instance.getJsonArray("identifiers"));

      assertThat(identifiers.size(), is(greaterThan(0)));

      identifiers.stream().forEach(identifier -> {
        assertThat("instance identifier should have a type",
          identifier.containsKey("identifierTypeId"), is(true));

        //Identifier type should be derived from MODS data, however for the moment,
        //default to ISBN
        assertThat("instance identifier should be ISBN",
          identifier.getString("identifierTypeId"), is(ApiTestSuite.getIsbnIdentifierType()));

        assertThat("instance identifier should have a value",
          identifier.containsKey("value"), is(true));
      });

      List<JsonObject> contributors = JsonArrayHelper.toList(
        instance.getJsonArray("contributors"));

      contributors.stream().forEach(contributor -> {
        assertThat(contributor.containsKey("contributorNameTypeId"), is(true));

        //Identifier type should be derived from MODS data, however for the moment,
        //default to personal name
        assertThat(contributor.getString("contributorNameTypeId"),
          is(ApiTestSuite.getPersonalContributorNameType()));

        assertThat(contributor.containsKey("name"), is(true));
      });
    });
  }

  private void holdingsHaveExpectedProperties(List<JsonObject> holdings) {
    //TODO: Could be replaced with separate loop per property for clearer feedback
    holdings.stream().forEach(holding -> {
      assertThat("holding should have an ID",
        holding.containsKey("id"), is(true));

      assertThat("holding should have an instance",
        holding.containsKey("instanceId"), is(true));

      assertThat("holding should have a permanent location",
        holding.containsKey("permanentLocationId"), is(true));

      assertThat("holding permanent location should be main library",
        holding.getString("permanentLocationId"),
        is(ApiTestSuite.getMainLibraryLocation()));
    });
  }

  private void itemsHaveExpectedProperties(List<JsonObject> items) {
    //TODO: Could be replaced with separate loop per property for clearer feedback
    items.stream().forEach(item -> {
      assertThat("item should have an ID",
        item.containsKey("id"), is(true));

      assertThat("item should have a barcode",
        item.containsKey("barcode"), is(true));

      assertThat("item should have an instance",
        item.containsKey("instanceId"), is(true));

      assertThat("item should have a holding",
        item.containsKey("holdingsRecordId"), is(true));

      assertThat("item should have a status",
        item.getJsonObject("status").getString("name"), is("Available"));

      assertThat("item should be a book material type",
        item.getJsonObject("materialType").getString("id"),
        is(ApiTestSuite.getBookMaterialType()));

      assertThat("item should be a book material type",
        item.getJsonObject("materialType").getString("name"), is("Book"));
    });
  }

  private void itemsHaveExpectedDerivedProperties(List<JsonObject> items) {
    //TODO: Could be replaced with separate loop per property for clearer feedback
    items.stream().forEach(item -> {
      assertThat("item should not have a derived title",
        item.containsKey("title"), is(true));

      assertThat("item should have a derived permanent location",
        item.getJsonObject("permanentLocation").getString("name"),
        is("Main Library"));
    });
  }

  private void storedItemsDoNotHaveDerivedProperties()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    List<JsonObject> storedItems = itemsStorageClient.getAll();

    //TODO: Could be replaced with separate loop per property for clearer feedback
    storedItems.stream().forEach(item -> {
      assertThat("Stored item should not have a title",
        item.containsKey("title"), is(false));

      assertThat("Stored item should not have a permanent location",
        item.containsKey("permanentLocationId"), is(false));
    });
  }

  private static URL getIngestUrl() throws MalformedURLException {
    return new URL(String.format("%s/ingest/mods", ApiRoot.inventory()));
  }

  private File loadFileFromResource(String filename) {
    ClassLoader classLoader = getClass().getClassLoader();

    return new File(classLoader.getResource(filename).getFile());
  }

  private static boolean InstanceEntitled(
    JsonObject instance,
    String title) {

    return instance.getString("title").contains(title);
  }
}
