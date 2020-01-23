package api.items;

import static api.ApiTestSuite.USER_ID;
import static api.ApiTestSuite.getCanCirculateLoanType;
import static api.ApiTestSuite.getDvdMaterialType;
import static api.ApiTestSuite.getMainLibraryLocation;
import static api.ApiTestSuite.getReadingRoomLocation;
import static api.ApiTestSuite.getThirdFloorLocation;
import static api.support.InstanceSamples.girlOnTheTrain;
import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static api.support.http.BusinessLogicInterfaceUrls.items;
import static org.folio.inventory.domain.items.CirculationNote.DATE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.SOURCE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.folio.inventory.domain.user.Personal.FIRST_NAME_KEY;
import static org.folio.inventory.domain.user.Personal.LAST_NAME_KEY;
import static org.folio.inventory.domain.user.User.ID_KEY;
import static org.folio.inventory.domain.user.User.PERSONAL_KEY;
import static org.folio.util.StringUtil.urlEncode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static support.matchers.ResponseMatchers.hasValidationError;
import static support.matchers.TextDateTimeMatcher.withinSecondsAfter;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.junit.Assert;
import org.junit.Test;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ItemApiExamples extends ApiTests {

  private static final String LAST_CHECK_IN_FIELD = "lastCheckIn";
  private static final String USER_ID_FIELD = "staffMemberId";
  private static final String SERVICE_POINT_FIELD = "servicePointId";
  private static final String DATETIME_FIELD = "dateTime";

  private static final String CALL_NUMBER = "callNumber";
  private static final String CALL_NUMBER_SUFFIX = "callNumberSuffix";
  private static final String CALL_NUMBER_PREFIX = "callNumberPrefix";
  private static final String CALL_NUMBER_TYPE_ID = UUID.randomUUID().toString();

  public ItemApiExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void canCreateAnItemWithoutIDAndHRID()
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
      .temporarilyInReadingRoom()
      .canCirculate()
      .withItemLevelCallNumber(CALL_NUMBER)
      .withItemLevelCallNumberSuffix(CALL_NUMBER_SUFFIX)
      .withItemLevelCallNumberPrefix(CALL_NUMBER_PREFIX)
      .withItemLevelCallNumberTypeId(CALL_NUMBER_TYPE_ID)
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .temporarilyCourseReserves()
      .withCopyNumber("cp")
    );

    assertCallNumbers(postResponse.getJson());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("id"), is(true));

    assertThat(createdItem.containsKey(Item.TAGS_KEY), is(true));

    final JsonObject tags = createdItem.getJsonObject(Item.TAGS_KEY);
    assertThat(tags.containsKey(Item.TAG_LIST_KEY), is(true));

    final JsonArray tagList = tags.getJsonArray(Item.TAG_LIST_KEY);
    assertThat((ArrayList<String>)tagList.getList(), hasItem("test-tag"));

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

    assertThat("Item should not have permanent location",
      createdItem.containsKey("permanentLocation"), is(false));

    assertThat(createdItem.getJsonObject("temporaryLocation").getString("name"), is("Reading Room"));

    assertCallNumbers(createdItem);

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
    assertThat(createdItem.getString("hrid"), notNullValue());
    assertThat(createdItem.getString("copyNumber"), is("cp"));
  }

  @Test
  public void canCreateItemWithAnIDAndHRID()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID itemId = UUID.randomUUID();
    final String hrid = "it777";

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .withId(itemId)
      .withHrid(hrid)
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .canCirculate()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag").add("test-tag2")))
      .temporarilyCourseReserves());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("id"), is(true));
    assertThat(createdItem.getString("id"), is(itemId.toString()));
    assertThat(createdItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdItem.getString("barcode"), is("645398607547"));
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject materialType = createdItem.getJsonObject("materialType");

    assertThat(createdItem.containsKey(Item.TAGS_KEY), is(true));

    final JsonObject tags = createdItem.getJsonObject(Item.TAGS_KEY);
    assertThat(tags.containsKey(Item.TAG_LIST_KEY), is(true));

    final JsonArray tagList = tags.getJsonArray(Item.TAG_LIST_KEY);
    assertThat(tagList.size(), is(2));
    assertThat((ArrayList<String>)tagList.getList(), hasItem("test-tag"));
    assertThat((ArrayList<String>)tagList.getList(), hasItem("test-tag2"));

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = createdItem.getJsonObject("permanentLoanType");

    JsonObject temporaryLoanType = createdItem.getJsonObject("temporaryLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));
    assertThat(temporaryLoanType.getString("id"), is(ApiTestSuite.getCourseReserveLoanType()));
    assertThat(temporaryLoanType.getString("name"), is("Course Reserves"));

    assertThat("Item should not have permanent location",
      createdItem.containsKey("permanentLocation"), is(false));

    assertThat(createdItem.getJsonObject("temporaryLocation").getString("name"), is("Reading Room"));

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
    assertThat(createdItem.getString("hrid"), is(hrid));
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
  public void cannotCreateAnItemWithoutStatus()
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

    JsonObject item = new ItemRequestBuilder()
      .forHolding(holdingId)
      .create();
    item.remove("status");

    CompletableFuture<Response> createCompleted = new CompletableFuture<>();
    okapiClient.post(items(""), item, ResponseHandler.json(createCompleted));

    Response createResponse = createCompleted.get(5, TimeUnit.SECONDS);
    assertThat(createResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  public void cannotCreateAnItemWithoutStatusName()
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

    JsonObject item = new ItemRequestBuilder()
      .forHolding(holdingId)
      .create();
    item.getJsonObject("status").remove("name");

    CompletableFuture<Response> createCompleted = new CompletableFuture<>();
    okapiClient.post(items(""), item, ResponseHandler.json(createCompleted));

    Response createResponse = createCompleted.get(5, TimeUnit.SECONDS);
    assertThat(createResponse,
      hasValidationError("Status is a required field", "status", null)
    );
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

    JsonObject lastCheckIn = new JsonObject()
      .put("servicePointId", "7c5abc9f-f3d7-4856-b8d7-6712462ca007")
      .put("staffMemberId", "12115707-d7c8-54e7-8287-22e97f7250a4")
      .put("dateTime", "2020-01-02T13:02:46.000Z");


    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));

    JsonObject updateItemRequest = newItemRequest.copy()
      .put("status", new JsonObject().put("name", "Checked out"))
      .put("copyNumber", "updatedCp")
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    itemsClient.replace(itemId, updateItemRequest);

    Response getResponse = itemsClient.getById(itemId);

    assertThat(getResponse.getStatusCode(), is(200));
    JsonObject updatedItem = getResponse.getJson();

    assertThat(updatedItem.containsKey(Item.TAGS_KEY), is(true));
    final JsonObject tags = updatedItem.getJsonObject(Item.TAGS_KEY);

    assertThat(tags.containsKey(Item.TAG_LIST_KEY), is(true));

    final JsonArray tagList = tags.getJsonArray(Item.TAG_LIST_KEY);
    assertThat((ArrayList<String>)tagList.getList(), hasItem(""));

    assertThat(updatedItem.containsKey("id"), is(true));
    assertThat(updatedItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(updatedItem.getString("barcode"), is("645398607547"));
    assertThat(updatedItem.getJsonObject("status").getString("name"), is("Checked out"));
    assertThat(updatedItem.getJsonObject(Item.LAST_CHECK_IN).getString("servicePointId"),
      is("7c5abc9f-f3d7-4856-b8d7-6712462ca007"));
    assertThat(updatedItem.getJsonObject(Item.LAST_CHECK_IN).getString("staffMemberId"),
      is("12115707-d7c8-54e7-8287-22e97f7250a4"));
    assertThat(updatedItem.getJsonObject(Item.LAST_CHECK_IN).getString("dateTime"),
      is("2020-01-02T13:02:46.000Z"));
    assertThat(updatedItem.getJsonObject("status").getString("name"), is("Checked out"));

    JsonObject materialType = updatedItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = updatedItem.getJsonObject("permanentLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));

    assertThat("Item should not have permanent location",
      updatedItem.containsKey("permanentLocation"), is(false));

    assertThat(updatedItem.getJsonObject("temporaryLocation").getString("name"), is("Reading Room"));

    selfLinkRespectsWayResourceWasReached(updatedItem);
    selfLinkShouldBeReachable(updatedItem);
    assertThat(updatedItem.getString("copyNumber"), is("updatedCp"));
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
      .temporarilyInReadingRoom()
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
        .forInstance(UUID.fromString(smallAngryInstance.getString("id")))
        .withCallNumber(CALL_NUMBER)
        .withCallNumberSuffix(CALL_NUMBER_SUFFIX)
        .withCallNumberPrefix(CALL_NUMBER_PREFIX)
        .withCallNumberTypeId(CALL_NUMBER_TYPE_ID)
    ).getId();

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
        .forInstance(UUID.fromString(girlOnTheTrainInstance.getString("id")))
        .withCallNumber(CALL_NUMBER)
        .withCallNumberSuffix(CALL_NUMBER_SUFFIX)
        .withCallNumberPrefix(CALL_NUMBER_PREFIX)
        .withCallNumberTypeId(CALL_NUMBER_TYPE_ID)
    ).getId();

    itemsClient.create(new ItemRequestBuilder()
      .forHolding(girlOnTheTrainHoldingId)
      .dvd()
      .canCirculate()
      .temporarilyCourseReserves()
      .withBarcode("645334645247"));

    JsonObject nodInstance = createInstance(nod(UUID.randomUUID()));

    UUID nodHoldingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(nodInstance.getString("id")))
        .withCallNumber(CALL_NUMBER)
        .withCallNumberSuffix(CALL_NUMBER_SUFFIX)
        .withCallNumberPrefix(CALL_NUMBER_PREFIX)
        .withCallNumberTypeId(CALL_NUMBER_TYPE_ID)
    ).getId();

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

    firstPageItems.forEach(ItemApiExamples::selfLinkRespectsWayResourceWasReached);
    firstPageItems.forEach(this::selfLinkShouldBeReachable);
    firstPageItems.forEach(ItemApiExamples::hasConsistentMaterialType);
    firstPageItems.forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    firstPageItems.forEach(ItemApiExamples::hasConsistentTemporaryLoanType);

    firstPageItems.forEach(ItemApiExamples::hasStatus);
    firstPageItems.forEach(ItemApiExamples::hasConsistentPermanentLocation);
    firstPageItems.forEach(ItemApiExamples::hasConsistentTemporaryLocation);
    firstPageItems.forEach(this::assertCallNumbers);

    secondPageItems.forEach(ItemApiExamples::selfLinkRespectsWayResourceWasReached);
    secondPageItems.forEach(this::selfLinkShouldBeReachable);
    secondPageItems.forEach(ItemApiExamples::hasConsistentMaterialType);
    secondPageItems.forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    secondPageItems.forEach(ItemApiExamples::hasConsistentTemporaryLoanType);
    secondPageItems.forEach(ItemApiExamples::hasStatus);
    secondPageItems.forEach(ItemApiExamples::hasConsistentPermanentLocation);
    secondPageItems.forEach(ItemApiExamples::hasConsistentTemporaryLocation);
    secondPageItems.forEach(this::assertCallNumbers);
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

    items.forEach(ItemApiExamples::hasConsistentPermanentLoanType);
    items.forEach(ItemApiExamples::hasConsistentTemporaryLoanType);
    items.forEach(ItemApiExamples::hasConsistentPermanentLocation);
    items.forEach(ItemApiExamples::hasConsistentTemporaryLocation);
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
      ResponseHandler.any(putItemCompleted));

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

  @Test
  public void canCreateAnItemWithACirculationNote()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    JsonObject user = new JsonObject()
      .put(ID_KEY, USER_ID)
      .put(PERSONAL_KEY, new JsonObject()
        .put(LAST_NAME_KEY, "Smith")
        .put(FIRST_NAME_KEY, "John"));

    JsonObject createdUser = usersClient.create(user).getJson();

    DateTime requestMade = DateTime.now();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .canCirculate()
      .temporarilyCourseReserves()
      .withCheckInNote());

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

    assertThat("Item should not have permanent location",
      createdItem.containsKey("permanentLocation"), is(false));

    assertThat(createdItem.getJsonObject("temporaryLocation").getString("name"), is("Reading Room"));

    JsonObject checkInNote = createdItem.getJsonArray(CIRCULATION_NOTES_KEY).getJsonObject(0);
    JsonObject source = checkInNote.getJsonObject(SOURCE_KEY);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
    assertThat(checkInNote.getString(DATE_KEY), withinSecondsAfter(Seconds.seconds(2), requestMade));

    assertThat(source.getString(ID_KEY), is(createdUser.getString(ID_KEY)));
    assertThat(source.getJsonObject(PERSONAL_KEY).getString(LAST_NAME_KEY),
      is(source.getJsonObject(PERSONAL_KEY).getString(LAST_NAME_KEY)));
    assertThat(source.getJsonObject(PERSONAL_KEY).getString(FIRST_NAME_KEY),
      is(source.getJsonObject(PERSONAL_KEY).getString(FIRST_NAME_KEY)));

    selfLinkRespectsWayResourceWasReached(createdItem);
    selfLinkShouldBeReachable(createdItem);
  }

  @Test
  public void canUpdateAnItemWithExistingCirculationNote()
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
      .temporarilyInReadingRoom()
      .withCheckInNote()
      .create();

    DateTime createItemRequestMade = DateTime.now();

    itemsClient.create(newItemRequest).getJson();

    JsonObject createdItem = itemsClient.getById(itemId).getJson();
    String createdItemCirculationNoteDate = createdItem
      .getJsonArray(CIRCULATION_NOTES_KEY)
      .getJsonObject(0)
      .getString(DATE_KEY);

    JsonObject updateItemRequest = newItemRequest.copy()
      .put("hrid", createdItem.getString("hrid"))
      .put("status", new JsonObject().put("name", "Checked out"));

    itemsClient.replace(itemId, updateItemRequest);

    Response getResponse = itemsClient.getById(itemId);

    assertThat(getResponse.getStatusCode(), is(200));
    JsonObject updatedItem = getResponse.getJson();

    assertThat(updatedItem.containsKey("id"), is(true));
    assertThat(updatedItem.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(updatedItem.getString("barcode"), is("645398607547"));
    assertThat(updatedItem.getJsonObject("status").getString("name"), is("Checked out"));

    JsonObject materialType = updatedItem.getJsonObject("materialType");

    assertThat(materialType.getString("id"), is(ApiTestSuite.getBookMaterialType()));
    assertThat(materialType.getString("name"), is("Book"));

    JsonObject permanentLoanType = updatedItem.getJsonObject("permanentLoanType");

    assertThat(permanentLoanType.getString("id"), is(ApiTestSuite.getCanCirculateLoanType()));
    assertThat(permanentLoanType.getString("name"), is("Can Circulate"));

    assertThat("Item should not have permanent location",
      updatedItem.containsKey("permanentLocation"), is(false));

    assertThat(updatedItem.getJsonObject("temporaryLocation").getString("name"), is("Reading Room"));

    JsonObject checkInNote = updatedItem.getJsonArray(CIRCULATION_NOTES_KEY).getJsonObject(0);

    assertThat(checkInNote.getString(DATE_KEY), not(createdItemCirculationNoteDate));
    assertThat(checkInNote.getString(DATE_KEY), withinSecondsAfter(Seconds.seconds(2), createItemRequestMade));

    selfLinkRespectsWayResourceWasReached(updatedItem);
    selfLinkShouldBeReachable(updatedItem);
  }

  @Test
  public void canPopulateLocationProperties() throws Exception {
    UUID itemId = UUID.randomUUID();

    JsonObject newItemRequest = new JsonObject()
      .put("id", itemId.toString())
      .put("status", new JsonObject().put("name", "Available"))
      .put("holdingsRecordId", createInstanceAndHolding().toString())
      .put("materialTypeId", getDvdMaterialType())
      .put("permanentLoanTypeId", getCanCirculateLoanType())
      .put("permanentLocationId", getMainLibraryLocation())
      .put("temporaryLocationId", getReadingRoomLocation());

    itemsStorageClient.create(newItemRequest);

    JsonObject item = itemsClient.getById(itemId).getJson();

    assertThat(item.getJsonObject("permanentLocation"), notNullValue());
    assertThat(item.getJsonObject("permanentLocation").getString("id"),
      is(getMainLibraryLocation()));
    assertThat(item.getJsonObject("permanentLocation").getString("name"),
      is("Main Library"));

    assertThat(item.getJsonObject("temporaryLocation"), notNullValue());
    assertThat(item.getJsonObject("temporaryLocation").getString("id"),
      is(getReadingRoomLocation()));
    assertThat(item.getJsonObject("temporaryLocation").getString("name"),
      is("Reading Room"));

    assertThat(item.getJsonObject("effectiveLocation"), notNullValue());
    assertThat(item.getJsonObject("effectiveLocation").getString("id"),
      is(getReadingRoomLocation()));
    assertThat(item.getJsonObject("effectiveLocation").getString("name"),
      is("Reading Room"));
  }

  @Test
  public void canSearchItemsByLocation() throws Exception {
    JsonObject readingRoomItem = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", "Available"))
      .put("holdingsRecordId", createInstanceAndHolding().toString())
      .put("materialTypeId", getDvdMaterialType())
      .put("permanentLoanTypeId", getCanCirculateLoanType())
      .put("permanentLocationId", getMainLibraryLocation())
      .put("temporaryLocationId", getReadingRoomLocation());

    JsonObject thirdFloorItem = readingRoomItem.copy()
      .put("id", UUID.randomUUID().toString())
      .put("temporaryLocationId", getThirdFloorLocation());

    JsonObject mainLibraryItem = readingRoomItem.copy()
      .put("id", UUID.randomUUID().toString());
      mainLibraryItem.remove("temporaryLocationId");

    itemsStorageClient.create(readingRoomItem);
    itemsStorageClient.create(thirdFloorItem);
    itemsStorageClient.create(mainLibraryItem);

    JsonObject readingRoomItems = findItems("effectiveLocationId=" + getReadingRoomLocation());
    JsonObject thirdFloorItems = findItems("effectiveLocationId=" + getThirdFloorLocation());
    JsonObject mainLibraryItems = findItems("effectiveLocationId=" + getMainLibraryLocation());

    assertThat(readingRoomItems.getInteger("totalRecords"), is(1));
    assertThat(readingRoomItems.getJsonArray("items").getJsonObject(0).getString("id"),
      is(readingRoomItem.getString("id")));

    assertThat(thirdFloorItems.getInteger("totalRecords"), is(1));
    assertThat(thirdFloorItems.getJsonArray("items").getJsonObject(0).getString("id"),
      is(thirdFloorItem.getString("id")));

    assertThat(mainLibraryItems.getInteger("totalRecords"), is(1));
    assertThat(mainLibraryItems.getJsonArray("items").getJsonObject(0).getString("id"),
      is(mainLibraryItem.getString("id")));
  }

  @Test
  public void itemHasLastCheckInPropertiesWhenTheyAreSet()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject readingRoomItem = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", "Available"))
      .put("holdingsRecordId", createInstanceAndHolding().toString())
      .put("materialTypeId", getDvdMaterialType())
      .put("permanentLoanTypeId", getCanCirculateLoanType())
      .put("permanentLocationId", getMainLibraryLocation())
      .put("temporaryLocationId", getReadingRoomLocation());

    JsonObject lastCheckInObj = new JsonObject();
    UUID userId = UUID.randomUUID();
    UUID servicePointId = UUID.randomUUID();
    DateTime checkInDate = DateTime.now();

    lastCheckInObj.put(SERVICE_POINT_FIELD, servicePointId.toString());
    lastCheckInObj.put(USER_ID_FIELD, userId.toString());
    lastCheckInObj.put(DATETIME_FIELD, checkInDate.toString());

    readingRoomItem.put(LAST_CHECK_IN_FIELD, lastCheckInObj);

    itemsStorageClient.create(readingRoomItem);

    JsonObject readingRoomItems = findItems("effectiveLocationId=" + getReadingRoomLocation());
    JsonObject actualItem = readingRoomItems.getJsonArray("items").getJsonObject(0);
    JsonObject actualLastCheckIn = actualItem.getJsonObject(LAST_CHECK_IN_FIELD);

    assertThat(actualLastCheckIn.getString(DATETIME_FIELD), is(checkInDate.toString()));
    assertThat(actualLastCheckIn.getString(SERVICE_POINT_FIELD), is(servicePointId.toString()));
    assertThat(actualLastCheckIn.getString(USER_ID_FIELD), is(userId.toString()));
  }

  @Test
  public void itemHasNoLastCheckInPropertiesWhenNotSet() throws Exception {
    JsonObject readingRoomItem = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("status", new JsonObject().put("name", "Available"))
        .put("holdingsRecordId", createInstanceAndHolding().toString())
        .put("materialTypeId", getDvdMaterialType())
        .put("permanentLoanTypeId", getCanCirculateLoanType())
        .put("permanentLocationId", getMainLibraryLocation())
        .put("temporaryLocationId", getReadingRoomLocation());

    itemsStorageClient.create(readingRoomItem);

    JsonObject readingRoomItems = findItems("effectiveLocationId=" + getReadingRoomLocation());

    JsonObject actualItem = readingRoomItems.getJsonArray("items").getJsonObject(0);
    assertThat(actualItem, is(notNullValue()));
    assertThat(actualItem.getJsonObject(LAST_CHECK_IN_FIELD), is(nullValue()));
  }

  @Test
  public void cannotChangeHRID() throws Exception {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getString("hrid"), notNullValue());

    JsonObject updatedItem = createdItem.copy()
      .put("barcode", "645398607548")
      .put("itemLevelCallNumber", "callNumber")
      .put("hrid", "updatedHrid");

    Response updateResponse = updateItem(updatedItem);

    assertThat(updateResponse,
      hasValidationError("HRID can not be updated", "hrid", "updatedHrid")
    );

    JsonObject existingItem = itemsClient.getById(postResponse.getId()).getJson();
    assertThat(existingItem, is(createdItem));
  }

  @Test
  public void cannotRemoveHRID() throws Exception {
    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withHrid("it777")
      .withBarcode("645398607547")
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getString("hrid"), notNullValue());

    JsonObject updatedItem = createdItem.copy()
      .put("barcode", "645398607548")
      .put("itemLevelCallNumber", "callNumber");

    updatedItem.remove("hrid");

    Response updateResponse = updateItem(updatedItem);

    assertThat(updateResponse,
      hasValidationError("HRID can not be updated", "hrid", null)
    );

    JsonObject existingItem = itemsClient.getById(postResponse.getId()).getJson();
    assertThat(existingItem, is(createdItem));
  }

  @Test
  public void cannotCreateItemWithUnrecognisedStatusName()
    throws InterruptedException, ExecutionException, TimeoutException {

    JsonObject itemWithUnrecognizedStatus = new ItemRequestBuilder()
      .forHolding(UUID.randomUUID())
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .create()
      .put("status", new JsonObject().put("name", "Unrecognized name"));

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(items(""),
      itemWithUnrecognizedStatus,
      ResponseHandler.any(postCompleted)
    );

    Response response = postCompleted.get(5, TimeUnit.SECONDS);
    assertThat(response, hasValidationError(
      "Undefined status specified",
      "status.name",
      "Unrecognized name"
    ));
  }

  @Test
  public void cannotUpdateItemWithUnrecognisedStatusName()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy()
      .put("status", new JsonObject().put("name", "Unrecognized name"));

    Response updateResponse = updateItem(updatedItem);
    assertThat(updateResponse, hasValidationError(
      "Undefined status specified",
      "status.name",
      "Unrecognized name"
    ));
  }

  @Test
  public void cannotRemoveStatusFromItem()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy();
    updatedItem.remove("status");

    Response updateResponse = updateItem(updatedItem);
    assertThat(updateResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  public void cannotRemoveStatusNameFromItem()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy()
      .put("status", new JsonObject());

    Response updateResponse = updateItem(updatedItem);
    assertThat(updateResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  public void statusDatePropertyPresentOnStatusUpdated()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
      new HoldingRequestBuilder()
        .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .canCirculate()
      .temporarilyCourseReserves());

    assertThat(postResponse.getJson().getJsonObject("status").getString("name"),
      is("Available"));
    assertFalse(postResponse.getJson().getJsonObject("status").containsKey("date"));

    final JsonObject itemToUpdate = postResponse.getJson().copy()
      .put("status", new JsonObject().put("name", "Checked out"));
    final DateTime beforeUpdateTime = DateTime.now(DateTimeZone.UTC);

    itemsClient.replace(postResponse.getId(), itemToUpdate);

    JsonObject updatedItem = itemsClient.getById(postResponse.getId()).getJson();
    assertThat(updatedItem.getJsonObject("status").getString("name"), is("Checked out"));
    assertThat(updatedItem.getJsonObject("status").getString("date"),
      withinSecondsAfter(Seconds.seconds(2), beforeUpdateTime)
    );
  }

  private Response updateItem(JsonObject item) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    String itemUpdateUri = String.format("%s/%s", ApiRoot.items(), item.getString("id"));
    CompletableFuture<Response> putItemCompleted = new CompletableFuture<>();

    okapiClient.put(itemUpdateUri, item, ResponseHandler.any(putItemCompleted));

    return putItemCompleted.get(5, TimeUnit.SECONDS);
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

    if (locationId.equals(ApiTestSuite.getThirdFloorLocation())) {
      assertThat(location.getString("id"), is(ApiTestSuite.getThirdFloorLocation()));
      assertThat(location.getString("name"), is("3rd Floor"));

    } else if (locationId.equals(ApiTestSuite.getMezzanineDisplayCaseLocation())) {
      assertThat(location.getString("id"), is(ApiTestSuite.getMezzanineDisplayCaseLocation()));
      assertThat(location.getString("name"), is("Display Case, Mezzanine"));

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

  private UUID createInstanceAndHolding()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();

    createInstance(smallAngryPlanet(instanceId));
    return holdingsStorageClient
      .create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }

  private JsonObject findItems(String searchQuery)
    throws InterruptedException, TimeoutException, ExecutionException {

    CompletableFuture<Response> getCompletedFuture = new CompletableFuture<>();
    okapiClient.get(items("?query=") + urlEncode(searchQuery),
      ResponseHandler.json(getCompletedFuture));

    return getCompletedFuture.get(5, TimeUnit.SECONDS).getJson();
  }

  private void assertCallNumbers(JsonObject item) {
    JsonObject callNumberComponents = item.getJsonObject("effectiveCallNumberComponents");

    assertNotNull(callNumberComponents);

    assertThat(callNumberComponents.getString("callNumber"), is(CALL_NUMBER));
    assertThat(callNumberComponents.getString("suffix"), is(CALL_NUMBER_SUFFIX));
    assertThat(callNumberComponents.getString("prefix"), is(CALL_NUMBER_PREFIX));
    assertThat(callNumberComponents.getString("typeId"), is(CALL_NUMBER_TYPE_ID));
  }
}
