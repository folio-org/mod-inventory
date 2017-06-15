package api

import api.support.ApiRoot
import api.support.InstanceApiClient
import api.support.ItemApiClient
import api.support.Preparation
import io.vertx.core.json.JsonObject
import org.folio.inventory.support.JsonArrayHelper
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response
import org.folio.inventory.support.http.client.ResponseHandler
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import static api.support.InstanceSamples.*

class ItemApiExamples extends Specification {
  private final OkapiHttpClient okapiClient = ApiTestSuite.createOkapiHttpClient()

  def setup() {
    def preparation = new Preparation(okapiClient)

    preparation.deleteItems()
    preparation.deleteInstances()
  }

  void "Can create an item"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItemRequest = new JsonObject()
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("temporaryLoanType", courseReservesLoanType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.containsKey("id")
      assert createdItem.getString("title") == "Long Way to a Small Angry Planet"
      assert createdItem.getString("barcode") == "645398607547"
      assert createdItem.getJsonObject("status").getString("name") == "Available"
      assert createdItem.getJsonObject("materialType").getString("id") == ApiTestSuite.bookMaterialType
      assert createdItem.getJsonObject("materialType").getString("name") == "Book"
      assert createdItem.getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType
      assert createdItem.getJsonObject("permanentLoanType").getString("name") == "Can Circulate"
      assert createdItem.getJsonObject("temporaryLoanType").getString("id") == ApiTestSuite.courseReserveLoanType
      assert createdItem.getJsonObject("temporaryLoanType").getString("name") == "Course Reserves"
      assert createdItem.getJsonObject("location").getString("name") == "Annex Library"

      selfLinkRespectsWayResourceWasReached(createdItem)
      selfLinkShouldBeReachable(createdItem)
  }

  void "Can create an item with an ID"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def itemId = UUID.randomUUID().toString()

      def newItemRequest = new JsonObject()
        .put("id", itemId)
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("barcode", "645398607547")

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.getString("id") == itemId

      selfLinkRespectsWayResourceWasReached(createdItem)
      selfLinkShouldBeReachable(createdItem)
  }

  void "Can create an item based upon an instance"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItemRequest = new JsonObject()
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.containsKey("id")
      assert createdItem.getString("title") == "Long Way to a Small Angry Planet"
      assert createdItem.getString("instanceId") == createdInstance.id
      assert createdItem.getString("barcode") == "645398607547"
      assert createdItem.getJsonObject("status").getString("name") == "Available"
      assert createdItem.getJsonObject("materialType").getString("id") == ApiTestSuite.bookMaterialType
      assert createdItem.getJsonObject("materialType").getString("name") == "Book"
      assert createdItem.getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType
      assert createdItem.getJsonObject("permanentLoanType").getString("name") == "Can Circulate"
      assert createdItem.getJsonObject("location").getString("name") == "Annex Library"

      selfLinkRespectsWayResourceWasReached(createdItem)
      selfLinkShouldBeReachable(createdItem)
  }

  void "Can create an item without a barcode"() {
    given:
      def newItemRequest = new JsonObject()
        .put("title", "Nod")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.containsKey("barcode") == false
  }

  void "Can create multiple items without a barcode"() {

    def firstItemRequest = new JsonObject()
      .put("title", "Temeraire")
      .put("status", new JsonObject().put("name", "Available"))
      .put("location", new JsonObject().put("name", "Main Library"))
      .put("materialType", bookMaterialType())
      .put("permanentLoanType", canCirculateLoanType())

    ItemApiClient.createItem(okapiClient, firstItemRequest)

    def newItemRequest = new JsonObject()
      .put("title", "Nod")
      .put("status", new JsonObject().put("name", "Available"))
      .put("materialType", bookMaterialType())
      .put("permanentLoanType", canCirculateLoanType())
      .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.containsKey("barcode") == false
  }

  void "Cannot create an item without a material type"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItemRequest = new JsonObject()
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("permanentLoanType", canCirculateLoanType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 422
  }

  void "Cannot create an item without a permanent loan type"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItemRequest = new JsonObject()
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 422
  }

  void "Can create an item without a temporary loan type"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItemRequest = new JsonObject()
        .put("title", createdInstance.title)
        .put("instanceId", createdInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("location", new JsonObject().put("name", "Annex Library"))

    when:
      def postCompleted = new CompletableFuture<Response>()

      okapiClient.post(ApiRoot.items(),
        newItemRequest, ResponseHandler.any(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert postResponse.statusCode == 201
      assert postResponse.location != null

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(postResponse.location, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def createdItem = getResponse.json

      assert createdItem.containsKey("id")
      assert createdItem.getString("title") == "Long Way to a Small Angry Planet"
      assert createdItem.getString("instanceId") == createdInstance.id
      assert createdItem.getString("barcode") == "645398607547"
      assert createdItem.getJsonObject("status").getString("name") == "Available"
      assert createdItem.getJsonObject("materialType").getString("id") == ApiTestSuite.bookMaterialType
      assert createdItem.getJsonObject("materialType").getString("name") == "Book"
      assert createdItem.getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType
      assert createdItem.getJsonObject("permanentLoanType").getString("name") == "Can Circulate"
      assert createdItem.getJsonObject("location").getString("name") == "Annex Library"

      selfLinkRespectsWayResourceWasReached(createdItem)
      selfLinkShouldBeReachable(createdItem)
  }

  void "Can update an existing item"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      def newItem = createItem(
        createdInstance.title, createdInstance.id, "645398607547")

      def updateItemRequest = newItem.copy()
        .put("status", new JsonObject().put("name", "Checked Out"))

      def itemLocation = new URL("${ApiRoot.items()}/${newItem.getString("id")}")

    when:
      def putCompleted = new CompletableFuture<Response>()

      okapiClient.put(itemLocation,
        updateItemRequest, ResponseHandler.any(putCompleted))

      Response putResponse = putCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert putResponse.statusCode == 204

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(itemLocation, ResponseHandler.json(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assert getResponse.statusCode == 200

      def updatedItem = getResponse.json

      assert updatedItem.containsKey("id")
      assert updatedItem.getString("title") == "Long Way to a Small Angry Planet"
      assert updatedItem.getString("instanceId") == createdInstance.id
      assert updatedItem.getString("barcode") == "645398607547"
      assert updatedItem.getJsonObject("status").getString("name") == "Checked Out"
      assert updatedItem.getJsonObject("materialType").getString("id") == ApiTestSuite.bookMaterialType
      assert updatedItem.getJsonObject("materialType").getString("name") == "Book"
      assert updatedItem.getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType
      assert updatedItem.getJsonObject("permanentLoanType").getString("name") == "Can Circulate"
      assert updatedItem.getJsonObject("location").getString("name") == "Main Library"

      selfLinkRespectsWayResourceWasReached(updatedItem)
      selfLinkShouldBeReachable(updatedItem)
  }

  void "Cannot update an item that does not exist"() {
    given:
      def updateItemRequest = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("title", "Nod")
        .put("instanceId", UUID.randomUUID().toString())
        .put("barcode", "546747342365")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("permanentLoanType", canCirculateLoanType())
        .put("location", new JsonObject().put("name", "Main Library"))

    when:
      def putCompleted = new CompletableFuture<Response>()

      okapiClient.put(new URL("${ApiRoot.items()}/${updateItemRequest.getString("id")}"),
        updateItemRequest, ResponseHandler.any(putCompleted))

      Response putResponse = putCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert putResponse.statusCode == 404
  }

  void "Can delete all items"() {
    given:
      def createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()))

      createItem(createdInstance.title, createdInstance.id, "645398607547")

      createItem(createdInstance.title, createdInstance.id, "175848607547")

      createItem(createdInstance.title, createdInstance.id, "645334645247")

    when:
      def deleteCompleted = new CompletableFuture<Response>()

      okapiClient.delete(ApiRoot.items(),
        ResponseHandler.any(deleteCompleted))

      Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert deleteResponse.statusCode == 204
      assert deleteResponse.hasBody() == false

      def getAllCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items(),
        ResponseHandler.json(getAllCompleted))

      Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

      assert getAllResponse.json.getJsonArray("items").size() == 0
      assert getAllResponse.json.getInteger("totalRecords") == 0
  }

  void "Can delete a single item"() {
    given:
      def createdInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      createItem(createdInstance.title, createdInstance.id, "645398607547")

      createItem(createdInstance.title, createdInstance.id, "175848607547")

      def itemToDelete = createItem(createdInstance.title, createdInstance.id,
        "645334645247")

      def itemToDeleteLocation =
        new URL("${ApiRoot.items()}/${itemToDelete.getString("id")}")

    when:
      def deleteCompleted = new CompletableFuture<Response>()

      okapiClient.delete(itemToDeleteLocation,
        ResponseHandler.any(deleteCompleted))

      Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS);

    then:
      assert deleteResponse.statusCode == 204
      assert deleteResponse.hasBody() == false

      def getCompleted = new CompletableFuture<Response>()

      okapiClient.get(itemToDeleteLocation,
        ResponseHandler.any(getCompleted))

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS)

      assert getResponse.statusCode == 404

      def getAllCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items(),
        ResponseHandler.json(getAllCompleted))

      Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

      assert getAllResponse.json.getJsonArray("items").size() == 2
      assert getAllResponse.json.getInteger("totalRecords") == 2
  }

  void "Can page all items"() {
    given:
      def smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()))

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "645398607547", bookMaterialType(), canCirculateLoanType(), null)

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "175848607547", bookMaterialType(), courseReservesLoanType(), null)

      def girlOnTheTrainInstance = createInstance(girlOnTheTrain(UUID.randomUUID()))

      createItem(girlOnTheTrainInstance.title, girlOnTheTrainInstance.id,
        "645334645247", dvdMaterialType(), canCirculateLoanType(),
        courseReservesLoanType())

      def nodInstance = createInstance(nod(UUID.randomUUID()))

      createItem(nodInstance.title, nodInstance.id, "564566456546")

      createItem(nodInstance.title, nodInstance.id, "943209584495")

    when:
      def firstPageGetCompleted = new CompletableFuture<Response>()
      def secondPageGetCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items("limit=3"),
        ResponseHandler.json(firstPageGetCompleted))

      okapiClient.get(ApiRoot.items("limit=3&offset=3"),
        ResponseHandler.json(secondPageGetCompleted))

      Response firstPageResponse = firstPageGetCompleted.get(5, TimeUnit.SECONDS)
      Response secondPageResponse = secondPageGetCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert firstPageResponse.statusCode == 200
      assert secondPageResponse.statusCode == 200

      def firstPageItems = JsonArrayHelper.toList(firstPageResponse.json.getJsonArray("items"))

      assert firstPageItems.size() == 3
      assert firstPageResponse.json.getInteger("totalRecords") == 5

      def secondPageItems = JsonArrayHelper.toList(secondPageResponse.json.getJsonArray("items"))

      assert secondPageItems.size() == 2
      assert secondPageResponse.json.getInteger("totalRecords") == 5

      firstPageItems.each {
        selfLinkRespectsWayResourceWasReached(it)
      }

      firstPageItems.each {
        selfLinkShouldBeReachable(it)
      }

      firstPageItems.each {
        hasConsistentMaterialType(it)
      }

      firstPageItems.each {
        hasConsistentPermanentLoanType(it)
      }

      firstPageItems.each {
        hasConsistentTemporaryLoanType(it)
      }

      firstPageItems.each {
        hasStatus(it)
      }

      firstPageItems.each {
        hasLocation(it)
      }

      secondPageItems.each {
        selfLinkRespectsWayResourceWasReached(it)
      }

      secondPageItems.each {
        selfLinkShouldBeReachable(it)
      }

      secondPageItems.each {
        hasConsistentMaterialType(it)
      }

      secondPageItems.each {
        hasConsistentPermanentLoanType(it)
      }

      secondPageItems.each {
        hasConsistentTemporaryLoanType(it)
      }

      secondPageItems.each {
        hasStatus(it)
      }

      secondPageItems.each {
        hasLocation(it)
      }
  }

  void "Can get all items with different permanent and temporary loan types"() {
    given:
      def smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()))

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "645398607547", bookMaterialType(), canCirculateLoanType(), null)

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "175848607547", bookMaterialType(), canCirculateLoanType(), courseReservesLoanType())

    when:
      def getAllCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items(),
        ResponseHandler.json(getAllCompleted))

      Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert getAllResponse.statusCode == 200

      def items = JsonArrayHelper.toList(getAllResponse.json.getJsonArray("items"))

      assert items.size() == 2
      assert getAllResponse.json.getInteger("totalRecords") == 2

      assert items.stream()
        .filter({it.getString("barcode") == "645398607547"})
        .findFirst().get().getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType

      assert items.stream()
        .filter({it.getString("barcode") == "645398607547"})
        .findFirst().get().containsKey("temporaryLoanType") == false

      assert items.stream()
        .filter({it.getString("barcode") == "175848607547"})
        .findFirst().get().getJsonObject("permanentLoanType").getString("id") == ApiTestSuite.canCirculateLoanType

      assert items.stream()
        .filter({it.getString("barcode") == "175848607547"})
        .findFirst().get().getJsonObject("temporaryLoanType").getString("id") == ApiTestSuite.courseReserveLoanType

      items.each {
        hasConsistentPermanentLoanType(it)
      }

      items.each {
        hasConsistentTemporaryLoanType(it)
      }
  }

  void "Page parameters must be numeric"() {
    when:
      def getPagedCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items("limit=&offset="),
        ResponseHandler.text(getPagedCompleted))

      Response getPagedResponse = getPagedCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert getPagedResponse.statusCode == 400
      assert getPagedResponse.body == "limit and offset must be numeric when supplied"
  }

  void "Can search for items by title"() {
    given:
      def smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()))

      createItem(smallAngryInstance.title, smallAngryInstance.id, "645398607547")

      def nodInstance = createInstance(nod(UUID.randomUUID()))

      createItem(nodInstance.title, nodInstance.id, "564566456546")

    when:
      def searchGetCompleted = new CompletableFuture<Response>()

      okapiClient.get(ApiRoot.items("query=title=*Small%20Angry*"),
        ResponseHandler.json(searchGetCompleted))

      Response searchGetResponse = searchGetCompleted.get(5, TimeUnit.SECONDS)

    then:
      assert searchGetResponse.statusCode == 200

      def items = JsonArrayHelper.toList(searchGetResponse.json.getJsonArray("items"))

      assert items.size() == 1
      assert searchGetResponse.json.getInteger("totalRecords") == 1

      def firstItem = items[0]

      assert firstItem.getString("title") == "Long Way to a Small Angry Planet"
      assert firstItem.getJsonObject("status").getString("name") == "Available"

      items.each {
        selfLinkRespectsWayResourceWasReached(it)
      }

      items.each {
        selfLinkShouldBeReachable(it)
      }

      items.each {
        hasConsistentMaterialType(it)
      }

      items.each {
        hasConsistentPermanentLoanType(it)
      }

      items.each {
        hasConsistentTemporaryLoanType(it)
      }

      items.each {
        hasStatus(it)
      }

      items.each {
        hasLocation(it)
      }
  }

  void "Cannot create a second item with the same barcode"() {
    given:
      def smallAngryInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "645398607547")

      def nodInstance = createInstance(nod(UUID.randomUUID()))

    when:
      def createItemCompleted = new CompletableFuture<Response>()

      def newItemRequest = new JsonObject()
        .put("title", nodInstance.title)
        .put("instanceId", nodInstance.id)
        .put("barcode", "645398607547")
        .put("status", new JsonObject().put("name", "Available"))
        .put("materialType", bookMaterialType())
        .put("location", new JsonObject().put("name", "Main Library"))

      okapiClient.post(ApiRoot.items(), newItemRequest,
        ResponseHandler.any(createItemCompleted))

    then:
      def createResponse = createItemCompleted.get(5, TimeUnit.SECONDS)

      assert createResponse.statusCode == 400
      assert createResponse.body == "Barcodes must be unique, 645398607547 is already assigned to another item"
  }

  void "Cannot update an item to have the same barcode as an existing item"() {
    given:
      def smallAngryInstance = createInstance(
        smallAngryPlanet(UUID.randomUUID()))

      createItem(smallAngryInstance.title, smallAngryInstance.id,
        "645398607547")

      def nodInstance = createInstance(nod(UUID.randomUUID()))

      def nodItem = createItem(nodInstance.title, nodInstance.id,
        "654647774352")

    when:
      def changedNodItem = nodItem.copy()
        .put("barcode", "645398607547")

      def nodItemLocation = new URL(
        "${ApiRoot.items()}/${changedNodItem.getString("id")}")

      def putItemCompleted = new CompletableFuture<Response>()

      okapiClient.put(nodItemLocation, changedNodItem,
        ResponseHandler.text(putItemCompleted));

    then:
      def putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS)

      assert putItemResponse.statusCode == 400
      assert putItemResponse.body == "Barcodes must be unique, 645398607547 is already assigned to another item"
  }

  void "Can change the barcode of an existing item"() {
      def nodInstance = createInstance(nod(UUID.randomUUID()))

      def nodItem = createItem(nodInstance.title, nodInstance.id,
        "654647774352")

    when:
      def changedNodItem = nodItem.copy()
        .put("barcode", "645398607547")

      def nodItemLocation = new URL(
        "${ApiRoot.items()}/${changedNodItem.getString("id")}")

    def putItemCompleted = new CompletableFuture<Response>()

    okapiClient.put(nodItemLocation, changedNodItem,
        ResponseHandler.any(putItemCompleted))

    then:
      def putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS)

      assert putItemResponse.statusCode == 204

      def getItemCompleted = new CompletableFuture<Response>()

      okapiClient.get(nodItemLocation, ResponseHandler.json(getItemCompleted))

      def getItemResponse = getItemCompleted.get(5, TimeUnit.SECONDS)

      assert getItemResponse.statusCode == 200

      assert getItemResponse.json.getString("barcode") == "645398607547"
  }

  void "Can remove the barcode of an existing item"() {
      def nodInstance = createInstance(nod(UUID.randomUUID()))

      def nodItem = createItem(nodInstance.title, nodInstance.id,
        "654647774352")

    def smallAngryInstance = createInstance(
      smallAngryPlanet(UUID.randomUUID()))

    //Second item with no barcode, to ensure empty barcode doesn't match
    createItem(smallAngryInstance.title, smallAngryInstance.id,
      null)

    when:
      def changedNodItem = nodItem.copy()

      changedNodItem.remove("barcode")

      def nodItemLocation = new URL(
        "${ApiRoot.items()}/${changedNodItem.getString("id")}")

      def putItemCompleted = new CompletableFuture<Response>()

      okapiClient.put(nodItemLocation, changedNodItem,
        ResponseHandler.any(putItemCompleted))

    then:
      def putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS)

      assert putItemResponse.statusCode == 204

      def getItemCompleted = new CompletableFuture<Response>()

      okapiClient.get(nodItemLocation, ResponseHandler.json(getItemCompleted))

      def getItemResponse = getItemCompleted.get(5, TimeUnit.SECONDS)

      assert getItemResponse.statusCode == 200

      assert getItemResponse.json.containsKey("barcode") == false
  }

  private void selfLinkRespectsWayResourceWasReached(JsonObject item) {
    assert containsApiRoot(item.getJsonObject("links").getString("self"))
  }

  private boolean containsApiRoot(String link) {
    link.contains(ApiTestSuite.apiRoot())
  }

  private void selfLinkShouldBeReachable(JsonObject item) {
    def getCompleted = new CompletableFuture<Response>()

    okapiClient.get(item.getJsonObject("links").getString("self"),
      ResponseHandler.json(getCompleted))

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assert getResponse.statusCode == 200
  }

  private void hasStatus(JsonObject item) {
    assert item.containsKey("status")
    assert item.getJsonObject("status").containsKey("name")
  }

  private void hasConsistentMaterialType(JsonObject item) {
    def materialType = item.getJsonObject("materialType")

    switch(materialType.getString("id")) {
      case ApiTestSuite.bookMaterialType:
        assert materialType.getString("id") == ApiTestSuite.bookMaterialType
        assert materialType.getString("name") == "Book"
        break

      case ApiTestSuite.dvdMaterialType:
        assert materialType.getString("id") == ApiTestSuite.dvdMaterialType
        assert materialType.getString("name") == "DVD"
        break

      default:
        assert materialType.getString("id") == null
        assert materialType.getString("name") == null
    }
  }

  private void hasConsistentPermanentLoanType(JsonObject item) {
    hasConsistentLoanType(item.getJsonObject("permanentLoanType"))
  }

  private void hasConsistentTemporaryLoanType(JsonObject item) {
    hasConsistentLoanType(item.getJsonObject("temporaryLoanType"))
  }

  private void hasConsistentLoanType(JsonObject loanType) {
    if(loanType == null) {
      return
    }

    switch (loanType.getString("id")) {
      case ApiTestSuite.canCirculateLoanType:
        assert loanType.getString("id") == ApiTestSuite.canCirculateLoanType
        assert loanType.getString("name") == "Can Circulate"
        break

      case ApiTestSuite.courseReserveLoanType:
        assert loanType.getString("id") == ApiTestSuite.courseReserveLoanType
        assert loanType.getString("name") == "Course Reserves"
        break

      default:
        assert loanType.getString("id") == null
        assert loanType.getString("name") == null
    }
  }

  private void hasLocation(JsonObject item) {
    assert item.getJsonObject("location").getString("name") != null
  }

  private def createInstance(JsonObject newInstanceRequest) {
    InstanceApiClient.createInstance(okapiClient, newInstanceRequest)
  }

  private JsonObject createItem(String title, String instanceId, String barcode) {
    createItem(title, instanceId, barcode, bookMaterialType(), canCirculateLoanType(), null)
  }

  private JsonObject createItem(
    String title,
    String instanceId,
    String barcode,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType) {

    def newItemRequest = new JsonObject()
      .put("title", title)
      .put("instanceId", instanceId)
      .put("status", new JsonObject().put("name", "Available"))
      .put("location", new JsonObject().put("name", "Main Library"))

    if(barcode != null) {
      newItemRequest.put("barcode", barcode)
    }

    if(materialType != null) {
      newItemRequest.put("materialType", materialType)
    }

    if(permanentLoanType != null) {
      newItemRequest.put("permanentLoanType", permanentLoanType)
    }

    if(temporaryLoanType != null) {
      newItemRequest.put("temporaryLoanType", temporaryLoanType)
    }

    ItemApiClient.createItem(okapiClient, newItemRequest)
  }

  private JsonObject bookMaterialType() {
    new JsonObject()
      .put("id", "${ApiTestSuite.bookMaterialType}")
      .put("name", "Book")
  }

  private JsonObject dvdMaterialType() {
    new JsonObject()
      .put("id", "${ApiTestSuite.dvdMaterialType}")
      .put("name", "DVD")
  }

  private JsonObject canCirculateLoanType() {
    new JsonObject()
      .put("id", "${ApiTestSuite.canCirculateLoanType}")
      .put("name", "Can Circulate")
  }

  private JsonObject courseReservesLoanType() {
    new JsonObject()
      .put("id", "${ApiTestSuite.courseReserveLoanType}")
      .put("name", "Course Reserves")
  }
}
