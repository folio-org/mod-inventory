package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.Preparation;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.awaitility.Duration;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class ModsIngestExamples extends ApiTests {
  private final OkapiHttpClient okapiClient;

  public ModsIngestExamples() throws MalformedURLException {
    okapiClient = ApiTestSuite.createOkapiHttpClient();
  }

  @Before
  public void setup()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    Preparation preparation = new Preparation(okapiClient);
    preparation.deleteInstances();
    preparation.deleteItems();
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
      .header("X-Okapi-Url", ApiTestSuite.storageOkapiUrl())
      .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
      .header("X-Okapi-Token", ApiTestSuite.TOKEN)
      .multiPart("record", modsFile)
      .when().post(getIngestUrl())
      .then()
      .log().all()
      .statusCode(202)
      .extract().header("location");

    await().atMost(new Duration(10, TimeUnit.SECONDS))
      .untilAsserted(() -> {
        ingestJobHasCompleted(statusLocation);
        expectedInstancesCreatedFromIngest();
        expectedItemsCreatedFromIngest();
      });
  }

  @Test
  public void willRefuseIngestForMultipleFiles()
    throws MalformedURLException {

    File modsFile = loadFileFromResource("mods/multiple-example-mods-records.xml");

    given()
      .header("X-Okapi-Url", ApiTestSuite.storageOkapiUrl())
      .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
      .header("X-Okapi-Token", ApiTestSuite.TOKEN)
      .multiPart("record", modsFile)
      .multiPart("record", modsFile)
      .when().post(getIngestUrl())
      .then()
      .statusCode(400)
      .body(is("Cannot parse multiple files in a single request"));
  }

  private void ingestJobHasCompleted(String statusLocation)
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(statusLocation, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));
    assertThat(getResponse.getJson().getString("status"), is("Completed"));
  }

  private void expectedItemsCreatedFromIngest()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    JsonObject wrappedItems = getAllResponse.getJson();

    List<JsonObject> items = JsonArrayHelper.toList(
      wrappedItems.getJsonArray("items"));

    assertThat(wrappedItems.getInteger("totalRecords"), is(9));

    //TODO: Could be replaced with separate loop per property for clearer feedback
    items.stream().forEach(item -> {
      assertThat(item.containsKey("id"), is(true));
      assertThat(item.containsKey("title"), is(true));
      assertThat(item.containsKey("barcode"), is(true));
      assertThat(item.containsKey("instanceId"), is(true));
      assertThat(item.getJsonObject("status").getString("name"), is("Available"));

      assertThat(item.getJsonObject("materialType")
        .getString("id"), is(ApiTestSuite.getBookMaterialType()));

      assertThat(item.getJsonObject("materialType").getString("name"), is("Book"));

      assertThat(item.getJsonObject("permanentLocation").getString("name"),
        is("Main Library"));
    });

    assertThat(items.stream().anyMatch( item ->
      itemSimilarTo(item, "California: its gold and its inhabitants", "69228882")),
      is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Studien zur Geschichte der Notenschrift.", "69247446")),
      is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Essays on C.S. Lewis and George MacDonald", "53556908")),
      is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Statistical sketches of Upper Canada", "69077747")),
      is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Edward McGuire, RHA", "22169083")), is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Influenza della Poesia sui Costumi", "43620390")),
      is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Pavle Nik", "37696876")), is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Grammaire", "69250051")), is(true));

    assertThat(items.stream().anyMatch(item ->
      itemSimilarTo(item, "Angry Planet", "67437645")), is(true));

    items.stream().forEach(item -> {
      try {
        hasCorrectInstanceRelationship(item);
      } catch (Exception e) {
        Assert.fail(e.toString());
      }
    });
  }

  private void hasCorrectInstanceRelationship(JsonObject item)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(new URL(String.format("%s/%s", ApiRoot.instances(),
      item.getString("instanceId"))), ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));
    assertThat(getResponse.getJson().getString("title"), is(item.getString("title")));
  }

  private void expectedInstancesCreatedFromIngest()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    JsonObject wrappedInstances = getAllResponse.getJson();

    List<JsonObject> instances = JsonArrayHelper.toList(
      wrappedInstances.getJsonArray("instances"));

    assertThat(wrappedInstances.getInteger("totalRecords"), is(9));

    //TODO: Could be replaced with separate loop per property for clearer feedback
    instances.stream().forEach(instance -> {
      assertThat(instance.containsKey("id"), is(true));
      assertThat(instance.containsKey("title"), is(true));

      assertThat(instance.containsKey("instanceTypeId"), is(true));
      assertThat(instance.getString("instanceTypeId"), is(ApiTestSuite.getBooksInstanceType()));

      assertThat(instance.containsKey("source"), is(true));

      assertThat(instance.containsKey("identifiers"), is(true));

      List<JsonObject> identifiers = JsonArrayHelper.toList(
        instance.getJsonArray("identifiers"));

      assertThat(identifiers.size(), is(greaterThan(0)));

      identifiers.stream().forEach(identifier -> {
        assertThat(identifier.containsKey("identifierTypeId"), is(true));
        //Identifier type should be derived from MODS data, however for the moment,
        //default to ISBN
        assertThat(identifier.getString("identifierTypeId"), is(ApiTestSuite.getIsbnIdentifierType()));

        assertThat(identifier.containsKey("value"), is(true));
      });

      List<JsonObject> creators = JsonArrayHelper.toList(
        instance.getJsonArray("creators"));

      creators.stream().forEach(creator -> {
        assertThat(creator.containsKey("creatorTypeId"), is(true));
        //Identifier type should be derived from MODS data, however for the moment,
        //default to Personal name
        assertThat(creator.getString("creatorTypeId"), is(ApiTestSuite.getPersonalCreatorType()));

        assertThat(creator.containsKey("name"), is(true));
      });
    });

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "California: its gold and its inhabitants",
        "Huntley, Henry Veel")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Studien zur Geschichte der Notenschrift.",
        "Riemann, Karl Wilhelm J. Hugo.")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Essays on C.S. Lewis and George MacDonald",
        "Marshall, Cynthia.")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Statistical sketches of Upper Canada",
        "Dunlop, William")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Edward McGuire, RHA", "Fallon, Brian.")),
      is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Influenza della Poesia sui Costumi",
      "MABIL, Pier Luigi.")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Pavle Nik", "Božović, Ratko.")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Grammaire", "Riemann, Othon.")), is(true));

    assertThat(instances.stream().anyMatch(instance ->
      InstanceSimilarTo(instance, "Angry Planet", "Unknown creator")), is(true));
  }

  private static URL getIngestUrl() throws MalformedURLException {
    return new URL(String.format("%s/ingest/mods", ApiRoot.inventory()));
  }

  private File loadFileFromResource(String filename) {
    ClassLoader classLoader = getClass().getClassLoader();

    return new File(classLoader.getResource(filename).getFile());
  }

  private static boolean itemSimilarTo(
    JsonObject record,
    String expectedSimilarTitle,
    String expectedBarcode) {

    return record.getString("title").contains(expectedSimilarTitle) &&
      StringUtils.equals(record.getString("barcode"), expectedBarcode);
  }

  private static boolean InstanceSimilarTo(
    JsonObject record,
    String expectedSimilarTitle,
    String expectedCreatorSimilarTo) {

    return record.getString("title").contains(expectedSimilarTitle) &&
      JsonArrayHelper.toList(record.getJsonArray("creators")).stream()
        .anyMatch(creator -> creator.getString("name")
          .contains(expectedCreatorSimilarTo));
  }
}
