package api

import api.support.ApiRoot
import api.support.Preparation
import io.vertx.core.json.JsonObject
import org.folio.inventory.support.JsonArrayHelper
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response
import org.folio.inventory.support.http.client.ResponseHandler
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import static io.restassured.RestAssured.given

class ModsIngestExamples extends Specification {
  private final OkapiHttpClient okapiClient = ApiTestSuite.createOkapiHttpClient()


  def setup() {
    def preparation = new Preparation(okapiClient)
    preparation.deleteInstances()
    preparation.deleteItems()
  }

  void "Ingest some MODS records"() {
    given:
      def modsFile = loadFileFromResource(
        "mods/multiple-example-mods-records.xml")

    when:
      def statusLocation = given()
        .header("X-Okapi-Url", ApiTestSuite.storageOkapiUrl())
        .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
        .header("X-Okapi-Token", ApiTestSuite.TOKEN)
        .multiPart("record", modsFile)
        .when().post(getIngestUrl())
        .then()
        .statusCode(202)
        .extract().header("location")

    then:
      def conditions = new PollingConditions(
        timeout: 10, initialDelay: 1.0, factor: 1.25)

      conditions.eventually {
        ingestJobHasCompleted(statusLocation)
        expectedInstancesCreatedFromIngest()
        expectedItemsCreatedFromIngest()
      }
  }

  void "Refuse ingest for multiple files"() {
    given:
      def modsFile =
        loadFileFromResource("mods/multiple-example-mods-records.xml")

    when:
      given()
        .header("X-Okapi-Url", ApiTestSuite.storageOkapiUrl())
        .header("X-Okapi-Tenant", ApiTestSuite.TENANT_ID)
        .header("X-Okapi-Token", ApiTestSuite.TOKEN)
        .multiPart("record", modsFile)
        .multiPart("record", modsFile)
        .when().post(getIngestUrl())
        .then()
        .statusCode(400)
        .body(is("Cannot parse multiple files in a single request"))

    then:
      assert true
  }

  private ingestJobHasCompleted(String statusLocation) {
    def getCompleted = new CompletableFuture<Response>()

    okapiClient.get(statusLocation, ResponseHandler.json(getCompleted))

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS)

    assert getResponse.statusCode == 200
    assert getResponse.json.getString("status") == "Completed"
  }

  private expectedItemsCreatedFromIngest() {
    def getAllCompleted = new CompletableFuture<Response>()

    okapiClient.get(ApiRoot.items(),
      ResponseHandler.json(getAllCompleted))

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS)

    def items = JsonArrayHelper.toList(getAllResponse.json.getJsonArray("items"))

    assert items.size() == 8
    assert items.every({ it.containsKey("id") })
    assert items.every({ it.containsKey("title") })
    assert items.every({ it.containsKey("barcode") })
    assert items.every({ it.containsKey("instanceId") })
    assert items.every({ it.getJsonObject("status").getString("name") == "Available" })
    assert items.every({ it.getJsonObject("materialType").getString("id") == ApiTestSuite.bookMaterialType })
    assert items.every({ it.getJsonObject("materialType").getString("name") == "Book" })
    assert items.every({ it.getJsonObject("permanentLocation").getString("name") == "Main Library" })

    assert items.any({
      itemSimilarTo(it, "California: its gold and its inhabitants", "69228882")
    })

    assert items.any({
      itemSimilarTo(it, "Studien zur Geschichte der Notenschrift.", "69247446")
    })

    assert items.any({
      itemSimilarTo(it, "Essays on C.S. Lewis and George MacDonald", "53556908")
    })

    assert items.any({
      itemSimilarTo(it, "Statistical sketches of Upper Canada", "69077747")
    })

    assert items.any({
      itemSimilarTo(it, "Edward McGuire, RHA", "22169083")
    })

    assert items.any({
      itemSimilarTo(it, "Influenza della Poesia sui Costumi", "43620390")
    })

    assert items.any({
      itemSimilarTo(it, "Pavle Nik", "37696876")
    })

    assert items.any({
      itemSimilarTo(it, "Grammaire", "69250051")
    })

    items.every({ hasCorrectInstanceRelationship(it) })
  }

  private hasCorrectInstanceRelationship(JsonObject item) {
    def getCompleted = new CompletableFuture<Response>()

    okapiClient.get(new URL("${ApiRoot.instances()}/${item.getString("instanceId")}"),
      ResponseHandler.json(getCompleted))

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS)

    assert getResponse.statusCode == 200
    assert getResponse.json.getString("title") == item.getString("title")
  }

  private expectedInstancesCreatedFromIngest() {
    def getAllCompleted = new CompletableFuture<Response>()

    okapiClient.get(ApiRoot.instances(),
      ResponseHandler.json(getAllCompleted))

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS)

    def instances = JsonArrayHelper.toList(getAllResponse.json.getJsonArray("instances"))

    assert instances.size() == 8
    assert instances.every({ it.containsKey("id") })
    assert instances.every({ it.containsKey("title") })

    assert instances.every({ it.containsKey("instanceTypeId") })
    assert instances.every({ it.getString("instanceTypeId") == ApiTestSuite.booksInstanceType })

    assert instances.every({ it.containsKey("source") })

    assert instances.every({ it.containsKey("identifiers") })
    assert instances.every({ it.getJsonArray("identifiers").size() >= 1 })

    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("identifiers"))
        .every({ it.containsKey("identifierTypeId") })
    })

    //Identifier type should be derived from MODS data, however for the moment,
    //default to ISBN
    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("identifiers"))
        .every({ it.getString("identifierTypeId") == ApiTestSuite.isbnIdentifierType })
    })

    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("identifiers"))
        .every({ it.containsKey("value") })
    })

    assert instances.every({ it.containsKey("creators") })
    assert instances.every({ it.getJsonArray("creators").size() >= 1 })

    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("creators"))
        .every({ it.containsKey("creatorTypeId") })
    })

    //Creator type should be derived from MODS data, however for the moment,
    //default to personal
    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("creators"))
        .every({ it.getString("creatorTypeId") == ApiTestSuite.personalCreatorType })
    })

    assert instances.every({
      JsonArrayHelper.toList(it.getJsonArray("creators"))
        .every({ it.containsKey("name") })
    })

    assert instances.any({
      InstanceSimilarTo(it, "California: its gold and its inhabitants")
    })

    assert instances.any({
      InstanceSimilarTo(it, "Studien zur Geschichte der Notenschrift.")
    })

    assert instances.any({
      InstanceSimilarTo(it, "Essays on C.S. Lewis and George MacDonald")
    })

    assert instances.any({
      InstanceSimilarTo(it, "Statistical sketches of Upper Canada")
    })

    assert instances.any({
      InstanceSimilarTo(it, "Edward McGuire, RHA")
    })

    assert instances.any({
      InstanceSimilarTo(it, "Influenza della Poesia sui Costumi") })

    assert instances.any({
      InstanceSimilarTo(it, "Pavle Nik") })

    assert instances.any({
      InstanceSimilarTo(it, "Grammaire") })
  }

  private URL getIngestUrl() {
    new URL("${ApiRoot.inventory()}/ingest/mods")
  }

  private File loadFileFromResource(String filename) {
    ClassLoader classLoader = getClass().getClassLoader();

    new File(classLoader.getResource(filename).getFile())
  }

  private boolean itemSimilarTo(
    JsonObject record,
    String expectedSimilarTitle,
    String expectedBarcode) {

    record.getString("title").contains(expectedSimilarTitle) &&
      record.getString("barcode") == expectedBarcode
  }

  private boolean InstanceSimilarTo(JsonObject record, String expectedSimilarTitle) {
    record.getString("title").contains(expectedSimilarTitle)
  }
}
