package api.items;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.ItemApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.support.InstanceSamples.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ItemApiExamples extends ApiTests {
  public ItemApiExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void canCreateAnItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInAnnex()
      .canCirculate()
      .temporarilyCourseReserves());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("id"), is(true));
    assertThat(createdItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdItem.getString("barcode"), is("645398607547"));
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject materialType = createdItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), CoreMatchers.is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = createdItem.getJsonObject("permanentLoanType");

    JsonObject temporaryLoanType = createdItem.getJsonObject("temporaryLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));

    assertThat(temporaryLoanType.getString("id"), is(ApiTestSuite.getCourseReserveLoanType()));
    assertThat(temporaryLoanType.getString("name"), is("Course Reserves"));

    assertThat(createdItem.getJsonObject("permanentLocation").getString("name"), is("Main Library"));
    assertThat(createdItem.getJsonObject("temporaryLocation").getString("name"), is("Annex Library"));

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
  }

  @Test
  public void canCreateItemWithAnID()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID itemId = UUID.randomUUID();

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInAnnex()
      .canCirculate()
      .temporarilyCourseReserves());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("id"), is(true));
    assertThat(createdItem.getString("id"), is(itemId.toString()));
    assertThat(createdItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdItem.getString("barcode"), is("645398607547"));
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject materialType = createdItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = createdItem.getJsonObject("permanentLoanType");

    JsonObject temporaryLoanType = createdItem.getJsonObject("temporaryLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));
    assertThat(temporaryLoanType.getString("id"), is(ApiTestSuite.getCourseReserveLoanType()));
    assertThat(temporaryLoanType.getString("name"), is("Course Reserves"));

    assertThat(createdItem.getJsonObject("permanentLocation").getString("name"), is("Main Library"));
    assertThat(createdItem.getJsonObject("temporaryLocation").getString("name"), is("Annex Library"));

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
  }

  @Test
  public void canCreateAnItemWithoutBarcode()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoBarcode());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("barcode"), is(false));
  }

  @Test
  public void canCreateMultipleItemsWithoutBarcode()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoBarcode());

    IndividualResource secondItemResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoBarcode());

    JsonObject createdItem = itemsClient.getById(secondItemResponse.getId()).getJson();

    assertThat(createdItem.containsKey("barcode"), is(false));
  }

  @Test
  public void cannotCreateItemWithoutMaterialType()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoMaterialType()
      .create();

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.items(), newItemRequest,
      ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(422));
  }

  @Test
  public void cannotCreateItemWithoutPermanentLoanType()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(
      smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoPermanentLoanType()
      .create();

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.items(), newItemRequest,
      ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(422));
  }

  @Test
  public void canCreateItemWithoutTemporaryLoanType()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(
      smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .withNoTemporaryLoanType());

    Response getResponse = itemsClient.getById(postResponse.getId());

    JsonObject createdItem = getResponse.getJson();

    assertThat(createdItem.containsKey("id"), is(true));
    assertThat(createdItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdItem.getString("barcode"), is("645398607547"));
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject materialType = createdItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = createdItem.getJsonObject("permanentLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));

    assertThat(createdItem.containsKey("temporaryLoanType"), is(false));

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
  }

  @Test
  public void canUpdateExistingItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(
      smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    UUID itemId = UUID.randomUUID();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInAnnex()
      .create();

    itemsClient.create(newItemRequest);

    JsonObject updateItemRequest = newItemRequest.copy()
      .put("status", new JsonObject().put("name", "Checked Out"));

    itemsClient.replace(itemId, updateItemRequest);

    Response getResponse = itemsClient.getById(itemId);

    assertThat(getResponse.getStatusCode(), is(200));
    JsonObject updatedItem = getResponse.getJson();

    assertThat(updatedItem.containsKey("id"), is(true));
    assertThat(updatedItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(updatedItem.getString("barcode"), is("645398607547"));
    assertThat(updatedItem.getJsonObject("status").getString("name"), is("Checked Out"));

    JsonObject materialType = updatedItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = updatedItem.getJsonObject("permanentLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));

    assertThat(updatedItem.getJsonObject("permanentLocation").getString("name"), is("Main Library"));
    assertThat(updatedItem.getJsonObject("temporaryLocation").getString("name"), is("Annex Library"));

    selfLinkRespectsWayResourceWasReached(updatedItem);
    selfLinkShouldBeReachable(updatedItem);
  }

  @Test
  public void cannotUpdateItemThatDoesNotExist()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(
      smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    UUID itemId = UUID.randomUUID();

    JsonObject updateItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .canCirculate()
      .temporarilyInAnnex()
      .create();

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    okapiClient.put(new URL(String.format("%s/%s", ApiRoot.items(),
      updateItemRequest.getString("id"))),
      updateItemRequest, ResponseHandler.any(putCompleted));

    Response putResponse = putCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putResponse.getStatusCode(), is(404));
  }

  @Test
  public void canDeleteAllItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("175848607547"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645334645247"));

    itemsClient.deleteAll();

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("items").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  public void CanDeleteSingleItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547"));

    IndividualResource itemToDeleteResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("175848607547"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645334645247"));

    itemsClient.delete(itemToDeleteResponse.getId());

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("items").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  public void canPageAllItems()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .courseReserves()
      .withBarcode("175848607547"));

    JsonObject girlOnTheTrainInstance = createInstance(girlOnTheTrain(UUID.randomUUID()));

    UUID girlOnTheTrainHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(girlOnTheTrainInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(girlOnTheTrainHoldingId)
      .dvd()
      .canCirculate()
      .temporarilyCourseReserves()
      .withBarcode("645334645247"));

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .courseReserves()
      .withBarcode("564566456546"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .courseReserves()
      .withBarcode("943209584495"));

    CompletableFuture<Response> firstPageGetCompleted = new CompletableFuture<>();
    CompletableFuture<Response> secondPageGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items("limit=3"),
      ResponseHandler.json(firstPageGetCompleted));

    okapiClient.get(ApiRoot.items("limit=3&offset=3"),
      ResponseHandler.json(secondPageGetCompleted));

    Response firstPageResponse = firstPageGetCompleted.get(5, TimeUnit.SECONDS);
    Response secondPageResponse = secondPageGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(firstPageResponse.getStatusCode(), is(200));
    assertThat(secondPageResponse.getStatusCode(), is(200));

    List<JsonObject> firstPageItems = JsonArrayHelper.toList(
      firstPageResponse.getJson().getJsonArray("items"));

    assertThat(firstPageItems.size(), is(3));
    assertThat(firstPageResponse.getJson().getInteger("totalRecords"), is(5));

    List<JsonObject> secondPageItems = JsonArrayHelper.toList(
      secondPageResponse.getJson().getJsonArray("items"));

    assertThat(secondPageItems.size(), is(2));
    assertThat(secondPageResponse.getJson().getInteger("totalRecords"), is(5));

    firstPageItems.stream().forEach(ItemApiExamples::selfLinkRespectsWayResourceWasReached);
    firstPageItems.stream().forEach(this::selfLinkShouldBeReachable);
    firstPageItems.stream().forEach(ItemApiExamples::hasConsistentMaterialType);
    firstPageItems.stream().forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    firstPageItems.stream().forEach(ItemApiExamples::hasConsistentTemporaryLoanType);

    firstPageItems.stream().forEach(ItemApiExamples::hasStatus);
    firstPageItems.stream().forEach(ItemApiExamples::hasConsistentPermanentLocation);
    firstPageItems.stream().forEach(ItemApiExamples::hasConsistentTemporaryLocation);

    secondPageItems.stream().forEach(ItemApiExamples::selfLinkRespectsWayResourceWasReached);
    secondPageItems.stream().forEach(this::selfLinkShouldBeReachable);
    secondPageItems.stream().forEach(ItemApiExamples::hasConsistentMaterialType);
    secondPageItems.stream().forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    secondPageItems.stream().forEach(ItemApiExamples::hasConsistentTemporaryLoanType);
    secondPageItems.stream().forEach(ItemApiExamples::hasStatus);
    secondPageItems.stream().forEach(ItemApiExamples::hasConsistentPermanentLocation);
    secondPageItems.stream().forEach(ItemApiExamples::hasConsistentTemporaryLocation);
  }

  @Test
  public void CanGetAllItemsWithDifferentTemporaryLoanType()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547"));

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .temporarilyCourseReserves()
      .withBarcode("175848607547"));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getStatusCode(), is(200));

    List<JsonObject> items = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("items"));

    assertThat(items.size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));

    assertThat(items.stream()
      .filter(item -> StringUtils.equals(item.getString("barcode"), "645398607547"))
      .findFirst().get().getJsonObject("permanentLoanType").getString("id"),
      is(ApiTestSuite.getCanCirculateLoanType()));

    assertThat(items.stream()
      .filter(item -> StringUtils.equals(item.getString("barcode"), "645398607547"))
      .findFirst().get().containsKey("temporaryLoanType"), is(false));

    assertThat(items.stream()
      .filter(item -> StringUtils.equals(item.getString("barcode"), "175848607547"))
      .findFirst().get().getJsonObject("permanentLoanType").getString("id"),
      is(ApiTestSuite.getCanCirculateLoanType()));

    assertThat(items.stream()
      .filter(item -> StringUtils.equals(item.getString("barcode"), "175848607547"))
      .findFirst().get().getJsonObject("temporaryLoanType").getString("id"),
      is(ApiTestSuite.getCourseReserveLoanType()));

    items.stream().forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    items.stream().forEach(ItemApiExamples::hasConsistentTemporaryLoanType);
    items.stream().forEach(ItemApiExamples::hasConsistentPermanentLocation);
    items.stream().forEach(ItemApiExamples::hasConsistentTemporaryLocation);
  }

  @Test
  public void pageParametersMustBeNumeric()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    CompletableFuture<Response> getPagedCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items("limit=&offset="),
      ResponseHandler.text(getPagedCompleted));

    Response getPagedResponse = getPagedCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getPagedResponse.getStatusCode(), is(400));
    assertThat(getPagedResponse.getBody(),
      is("limit and offset must be numeric when supplied"));
  }

  @Test
  public void cannotSearchForItemsByTitle()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547"));

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .canCirculate()
      .withBarcode("564566456546"));

    CompletableFuture<Response> searchGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.items("query=title=*Small%20Angry*"),
      ResponseHandler.json(searchGetCompleted));

    Response searchGetResponse = searchGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(searchGetResponse.getStatusCode(), is(200));

    List<JsonObject> items = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("items"));

    assertThat(items.size(), is(0));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  public void cannotCreateSecondItemWithSameBarcode()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547"));

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    JsonObject sameBarcodeItemRequest = new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547").create();

    CompletableFuture<Response> createItemCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.items(), sameBarcodeItemRequest,
      ResponseHandler.any(createItemCompleted));

    Response sameBarcodeCreateResponse = createItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(sameBarcodeCreateResponse.getStatusCode(), is(400));
    assertThat(sameBarcodeCreateResponse.getBody(),
      is("Barcode must be unique, 645398607547 is already assigned to another item"));
  }

  @Test
  public void cannotUpdateItemToSameBarcodeAsExistingItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withBarcode("645398607547"));

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    IndividualResource nodItemResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .canCirculate()
      .withBarcode("654647774352"));

    JsonObject changedNodItem = nodItemResponse.getJson().copy()
      .put("barcode", "645398607547");

    URL nodItemLocation = new URL(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId()));

    CompletableFuture<Response> putItemCompleted = new CompletableFuture<>();

    okapiClient.put(nodItemLocation, changedNodItem,
      ResponseHandler.text(putItemCompleted));

    Response putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putItemResponse.getStatusCode(), is(400));
    assertThat(putItemResponse.getBody(),
      is("Barcode must be unique, 645398607547 is already assigned to another item"));
  }

  @Test
  public void canChangeBarcodeForExistingItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    IndividualResource nodItemResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .canCirculate()
      .withBarcode("654647774352"));

    JsonObject changedNodItem = nodItemResponse.getJson().copy()
      .put("barcode", "645398607547");

    URL nodItemLocation = new URL(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId()));

    CompletableFuture<Response> putItemCompleted = new CompletableFuture<>();

    okapiClient.put(nodItemLocation, changedNodItem,
        ResponseHandler.any(putItemCompleted));

    Response putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putItemResponse.getStatusCode(), is(204));

    CompletableFuture<Response> getItemCompleted = new CompletableFuture<>();

    okapiClient.get(nodItemLocation, ResponseHandler.json(getItemCompleted));

    Response getItemResponse = getItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getItemResponse.getStatusCode(), is(200));
    assertThat(getItemResponse.getJson().getString("barcode"), is("645398607547"));
  }

  @Test
  public void canRemoveBarcodeFromAnExistingItem()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID smallAngryHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
      .getId();

    //Existing item with no barcode, to ensure empty barcode doesn't match
    itemsClient.create(new ItemRequestBuilder()
      .forHolding(smallAngryHoldingId)
      .book()
      .canCirculate()
      .withNoBarcode());

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id"))))
      .getId();

    IndividualResource nodItemResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(nodHoldingId)
      .book()
      .canCirculate()
      .withBarcode("654647774352"));

    JsonObject changedNodItem = nodItemResponse.getJson().copy();

    changedNodItem.remove("barcode");

    URL nodItemLocation = new URL(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId()));

    CompletableFuture<Response> putItemCompleted = new CompletableFuture<>();

    okapiClient.put(nodItemLocation, changedNodItem,
      ResponseHandler.any(putItemCompleted));

    Response putItemResponse = putItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putItemResponse.getStatusCode(), is(204));

    CompletableFuture<Response> getItemCompleted = new CompletableFuture<>();

    okapiClient.get(nodItemLocation, ResponseHandler.json(getItemCompleted));

    Response getItemResponse = getItemCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getItemResponse.getStatusCode(), is(200));
    assertThat(getItemResponse.getJson().containsKey("barcode"), is(false));
  }

  private static void selfLinkRespectsWayResourceWasReached(JsonObject item) {
    containsApiRoot(item.getJsonObject("links").getString("self"));
  }

  private static void containsApiRoot(String link) {
    assertThat(link, containsString(ApiTestSuite.apiRoot()));
  }

  private void selfLinkShouldBeReachable(JsonObject item) {
    try {
      CompletableFuture<Response> getCompleted = new CompletableFuture<>();

      okapiClient.get(item.getJsonObject("links").getString("self"),
        ResponseHandler.json(getCompleted));

      Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

      assertThat(getResponse.getStatusCode(), is(200));
    }
    catch(Exception e) {
      Assert.fail(e.toString());
    }
  }

  private static void hasStatus(JsonObject item) {
    assertThat(item.containsKey("status"), is(true));
    assertThat(item.getJsonObject("status").containsKey("name"), is(true));
  }

  private static void hasConsistentMaterialType(JsonObject item) {
    JsonObject materialType = item.getJsonObject("materialType");

    String materialTypeId = materialType.getString("id");

    if (materialTypeId.equals(ApiTestSuite.getBookMaterialType())) {
      assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
      assertThat(materialType.getString("name"), is("Book"));

    } else if (materialTypeId.equals(ApiTestSuite.getDvdMaterialType())) {
      assertThat(materialType.getString("id"), is(ApiTestSuite.getDvdMaterialType()));
      assertThat(materialType.getString("name"), is("DVD"));

    } else {
      assertThat(materialType.getString("id"), is(nullValue()));
      assertThat(materialType.getString("name"), is(nullValue()));
    }
  }

  private static void hasConsistentPermanentLoanType(JsonObject item) {
    hasConsistentLoanType(item.getJsonObject("permanentLoanType"));
  }

  private static void hasConsistentTemporaryLoanType(JsonObject item) {
    hasConsistentLoanType(item.getJsonObject("temporaryLoanType"));
  }

  private static void hasConsistentLoanType(JsonObject loanType) {
    if(loanType == null) {
      return;
    }

    String loanTypeId = loanType.getString("id");

    if (loanTypeId.equals(ApiTestSuite.getCanCirculateLoanType())) {
      assertThat(loanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
      assertThat(loanType.getString("name"), is("Can Circulate"));

    } else if (loanTypeId.equals(ApiTestSuite.getCourseReserveLoanType())) {
      assertThat(loanType.getString("id"), is(ApiTestSuite.getCourseReserveLoanType()));
      assertThat(loanType.getString("name"), is("Course Reserves"));

    } else {
      assertThat(loanType.getString("id"), is(nullValue()));
      assertThat(loanType.getString("name"), is(nullValue()));
    }
  }

  private static void hasConsistentPermanentLocation(JsonObject item) {
    hasConsistentLocation(item.getJsonObject("permanentLocation"));
  }

  private static void hasConsistentTemporaryLocation(JsonObject item) {
    hasConsistentLocation(item.getJsonObject("temporaryLocation"));
  }

  private static void hasConsistentLocation(JsonObject location) {
    if(location == null) {
      return;
    }

    String locationId = location.getString("id");

    if (locationId.equals(ApiTestSuite.getMainLibraryLocation())) {
      assertThat(location.getString("id"), is(ApiTestSuite.getMainLibraryLocation()));
      assertThat(location.getString("name"), is("Main Library"));

    } else if (locationId.equals(ApiTestSuite.getAnnexLocation())) {
      assertThat(location.getString("id"), is(ApiTestSuite.getAnnexLocation()));
      assertThat(location.getString("name"), is("Annex Library"));

    } else {
      assertThat(location.getString("id"), is(nullValue()));
      assertThat(location.getString("name"), is(nullValue()));
    }
  }

  private JsonObject createInstance(JsonObject newInstanceRequest)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  private JsonObject createItem(String title, String instanceId, String barcode)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return createItem(title, instanceId, barcode, bookMaterialType(),
      canCirculateLoanType(), null);
  }

  private JsonObject createItem(
    String title,
    String instanceId,
    String barcode,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject newItemRequest = new JsonObject()
      .put("title", title)
      .put("instanceId", instanceId)
      .put("pieceIdentifiers", new ArrayList<>())
      .put("notes", new ArrayList<>())
      .put("status", new JsonObject().put("name", "Available"))
      .put("permanentLocation", permanentLocation())
      .put("temporaryLocation", temporaryLocation());

    if(barcode != null) {
      newItemRequest.put("barcode", barcode);
    }

    if(materialType != null) {
      newItemRequest.put("materialType", materialType);
    }

    if(permanentLoanType != null) {
      newItemRequest.put("permanentLoanType", permanentLoanType);
    }

    if(temporaryLoanType != null) {
      newItemRequest.put("temporaryLoanType", temporaryLoanType);
    }

    return ItemApiClient.createItem(okapiClient, newItemRequest);
  }

  private static JsonObject bookMaterialType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getBookMaterialType())
      .put("name", "Book");
  }

  private static JsonObject dvdMaterialType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getDvdMaterialType())
      .put("name", "DVD");
  }

  private static JsonObject canCirculateLoanType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getCanCirculateLoanType())
      .put("name", "Can Circulate");
  }

  private static JsonObject courseReservesLoanType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getCourseReserveLoanType())
      .put("name", "Course Reserves");
  }

  private static JsonObject temporaryLocation() {
		return new JsonObject()
			.put("id", ApiTestSuite.getAnnexLocation())
			.put("name", "Annex Library");
  }

  private static JsonObject permanentLocation() {
		return new JsonObject()
			.put("id", ApiTestSuite.getMainLibraryLocation())
			.put("name", "Main Library");
	}
}
