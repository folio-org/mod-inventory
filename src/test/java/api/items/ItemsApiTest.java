package api.items;

import static api.ApiTestSuite.USER_ID;
import static api.ApiTestSuite.getCanCirculateLoanType;
import static api.ApiTestSuite.getDvdMaterialType;
import static api.ApiTestSuite.getMainLibraryLocation;
import static api.ApiTestSuite.getReadingRoomLocation;
import static api.ApiTestSuite.getThirdFloorLocation;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.domain.items.CirculationNote.DATE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.SOURCE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.BARCODE_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.inventory.domain.items.Item.ORDER_KEY;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.inventory.domain.user.Personal.FIRST_NAME_KEY;
import static org.folio.inventory.domain.user.Personal.LAST_NAME_KEY;
import static org.folio.inventory.domain.user.User.ID_KEY;
import static org.folio.inventory.domain.user.User.PERSONAL_KEY;
import static org.folio.util.StringUtil.urlEncode;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static support.fixtures.InstanceFixture.girlOnTheTrain;
import static support.fixtures.InstanceFixture.nod;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.http.BusinessLogicInterfaceUrls.items;
import static support.matchers.ItemMatchers.hasCallNumbers;
import static support.matchers.ItemMatchers.hasConsistentMaterialType;
import static support.matchers.ItemMatchers.hasConsistentPermanentLoanType;
import static support.matchers.ItemMatchers.hasConsistentPermanentLocation;
import static support.matchers.ItemMatchers.hasConsistentTemporaryLoanType;
import static support.matchers.ItemMatchers.hasConsistentTemporaryLocation;
import static support.matchers.ResponseMatchers.hasValidationError;
import static support.matchers.TextDateTimeMatcher.withinSecondsAfter;

import api.ApiTestSuite;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.lang3.Strings;
import org.folio.inventory.domain.items.CQLQueryRequestDto;
import org.folio.inventory.domain.items.EffectiveCallNumberComponents;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import support.ApiRoot;
import support.ApiTests;
import support.InstanceApiClient;
import support.builders.HoldingRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.fixtures.InstanceRequestFixture;
import support.fixtures.ItemRequestFixture;

public class ItemsApiTest extends ApiTests {

  private static final String LAST_CHECK_IN_FIELD = "lastCheckIn";
  private static final String USER_ID_FIELD = "staffMemberId";
  private static final String SERVICE_POINT_FIELD = "servicePointId";
  private static final String DATETIME_FIELD = "dateTime";

  private static final String CALL_NUMBER = "callNumber";
  private static final String CALL_NUMBER_SUFFIX = "callNumberSuffix";
  private static final String CALL_NUMBER_PREFIX = "callNumberPrefix";
  private static final String CALL_NUMBER_TYPE_ID = UUID.randomUUID().toString();

  @Test
  @SneakyThrows
  void canCreateAnItemWithoutIDAndHRID() {
    UUID holdingId = createInstanceAndHolding();
    String testNote = "this is a note";
    JsonArray adminNote = new JsonArray();
    adminNote.add(testNote);
    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .canCirculate()
      .withAdministrativeNotes(adminNote)
      .withItemLevelCallNumber(CALL_NUMBER)
      .withItemLevelCallNumberSuffix(CALL_NUMBER_SUFFIX)
      .withItemLevelCallNumberPrefix(CALL_NUMBER_PREFIX)
      .withItemLevelCallNumberTypeId(CALL_NUMBER_TYPE_ID)
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .temporarilyCourseReserves()
      .withCopyNumber("cp")
    );

    assertThat(postResponse.getJson(),
      hasCallNumbers(CALL_NUMBER, CALL_NUMBER_SUFFIX, CALL_NUMBER_PREFIX, CALL_NUMBER_TYPE_ID));

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("id"), is(true));

    assertThat(createdItem.containsKey("administrativeNotes"), is(true));

    List<String> createdNotes = JsonArrayHelper
      .toListOfStrings(createdItem.getJsonArray("administrativeNotes"));

    assertThat(createdNotes, contains(testNote));

    assertThat(createdItem.containsKey(Item.TAGS_KEY), is(true));

    assertThat(getTags(createdItem), hasItem("test-tag"));

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

    assertThat(createdItem,
      hasCallNumbers(CALL_NUMBER, CALL_NUMBER_SUFFIX, CALL_NUMBER_PREFIX, CALL_NUMBER_TYPE_ID));

    assertThat("Item should contain an effective shelving order",
      createdItem.containsKey("effectiveShelvingOrder"), is(true));

    assertThat(createdItem.getString("hrid"), notNullValue());
    assertThat(createdItem.getString("copyNumber"), is("cp"));
  }

  @Test
  @SneakyThrows
  void canCreateItemWithAnIDAndHRID() {
    UUID itemId = UUID.randomUUID();
    final String hrid = "it777";

    UUID holdingId = createInstanceAndHolding();

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

    assertThat(getTags(createdItem), hasItems("test-tag", "test-tag2"));

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

    assertThat(createdItem.getString("hrid"), is(hrid));
  }

  @Test
  @SneakyThrows
  void canCreateAnItemWithoutBarcode() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoBarcode());

    JsonObject createdItem = itemsClient.getById(postResponse.getId()).getJson();

    assertThat(createdItem.containsKey("barcode"), is(false));
  }

  @Test
  @SneakyThrows
  void canCreateMultipleItemsWithoutBarcode() {
    UUID holdingId = createInstanceAndHolding();

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
  @SneakyThrows
  void cannotCreateItemWithoutMaterialType() {
    UUID holdingId = createInstanceAndHolding();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoMaterialType()
      .create();

    final var postCompleted = okapiClient.post(ApiRoot.items(), newItemRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(postResponse.statusCode(), is(422));
  }

  @Test
  @SneakyThrows
  void cannotCreateItemWithInvalidOrder() {
    UUID holdingId = createInstanceAndHolding();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .forHolding(holdingId)
      .create();

    newItemRequest.put(ORDER_KEY, "invalid-order");

    final var postCompleted = okapiClient.post(ApiRoot.items(), newItemRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(postResponse.statusCode(), is(422));
  }

  @Test
  @SneakyThrows
  void cannotCreateItemWithoutPermanentLoanType() {
    UUID holdingId = createInstanceAndHolding();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .forHolding(holdingId)
      .withNoPermanentLoanType()
      .create();

    final var postCompleted = okapiClient.post(ApiRoot.items(), newItemRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(postResponse.statusCode(), is(422));
  }

  @Test
  @SneakyThrows
  void canCreateItemWithoutTemporaryLoanType() {
    UUID holdingId = createInstanceAndHolding();

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
  }

  @Test
  @SneakyThrows
  void cannotCreateAnItemWithoutStatus() {
    UUID holdingId = createInstanceAndHolding();
    JsonObject item = new ItemRequestBuilder()
      .forHolding(holdingId)
      .create();
    item.remove("status");

    final var createCompleted = okapiClient.post(items(""), item);

    Response createResponse = createCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(createResponse,
      hasValidationError("Status is a required field", "status", null));
  }

  @Test
  @SneakyThrows
  void cannotCreateAnItemWithoutStatusName() {
    UUID holdingId = createInstanceAndHolding();

    JsonObject item = new ItemRequestBuilder()
      .forHolding(holdingId)
      .create();
    item.getJsonObject("status").remove("name");

    final var createCompleted = okapiClient.post(items(""), item);

    Response createResponse = createCompleted.toCompletableFuture().get(5, SECONDS);
    assertThat(createResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  @SneakyThrows
  void canUpdateExistingItem() {
    UUID transitDestinationServicePointIdForCreate = UUID.randomUUID();
    UUID transitDestinationServicePointIdForUpdate = UUID.randomUUID();
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject lastCheckIn = defaultLastCheckIn();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));
    assertThat(newItemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForCreate.toString()));

    JsonObject updateItemRequest = newItemRequest.copy()
      .put("status", new JsonObject().put("name", "Checked out"))
      .put("copyNumber", "updatedCp")
      .put(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY, transitDestinationServicePointIdForUpdate)
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    itemsClient.replace(itemId, updateItemRequest);

    Response getResponse = itemsClient.getById(itemId);

    assertThat(getResponse.statusCode(), is(200));
    JsonObject updatedItem = getResponse.getJson();

    assertThat(getTags(updatedItem), hasItem(""));
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

    assertThat(updatedItem.getString("copyNumber"), is("updatedCp"));
    assertThat(updatedItem.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForUpdate.toString()));
  }

  @Test
  @SneakyThrows
  void cannotUpdateItemThatDoesNotExist() {
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject updateItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .canCirculate()
      .temporarilyInReadingRoom()
      .create();

    final var putCompleted = okapiClient.put(
      String.format("%s/%s", ApiRoot.items(), updateItemRequest.getString("id")),
      updateItemRequest);

    Response putResponse = putCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(putResponse.statusCode(), is(404));
  }

  @Test
  @SneakyThrows
  void cannotUpdateItemWithOptimisticLockingFailure() {
    UUID holdingId = createInstanceAndHolding();
    JsonObject item = new ItemRequestBuilder()
      .withId(ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE)
      .forHolding(holdingId)
      .canCirculate()
      .temporarilyInReadingRoom()
      .create();
    item = itemsClient.create(item).getJson();

    assertThat(itemsClient.attemptToReplace(UUID.fromString(item.getString("id")), item).statusCode(), is(409));
  }

  @Test
  @SneakyThrows
  void canDeleteAllItems() {
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

    final var getAllCompleted = okapiClient.get(ApiRoot.items());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("items").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  @SneakyThrows
  void canDeleteSingleItem() {
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

    final var getAllCompleted = okapiClient.get(ApiRoot.items());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("items").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  @SneakyThrows
  void canPageAllItems() {
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

    final var firstPageGetCompleted = okapiClient.get(ApiRoot.items("limit=3"));
    final var secondPageGetCompleted = okapiClient.get(ApiRoot.items("limit=3&offset=3"));

    Response firstPageResponse = firstPageGetCompleted.toCompletableFuture().get(5, SECONDS);
    Response secondPageResponse = secondPageGetCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(firstPageResponse.statusCode(), is(200));
    assertThat(secondPageResponse.statusCode(), is(200));

    List<JsonObject> firstPageItems = JsonArrayHelper.toList(
      firstPageResponse.getJson().getJsonArray("items"));

    assertThat(firstPageItems.size(), is(3));
    assertThat(firstPageResponse.getJson().getInteger("totalRecords"), is(5));

    List<JsonObject> secondPageItems = JsonArrayHelper.toList(
      secondPageResponse.getJson().getJsonArray("items"));

    assertThat(secondPageItems.size(), is(2));
    assertThat(secondPageResponse.getJson().getInteger("totalRecords"), is(5));

    firstPageItems.forEach(item -> assertThat(item, hasConsistentMaterialType()));
    firstPageItems.forEach(item -> assertThat(item, hasConsistentPermanentLoanType()));
    firstPageItems.forEach(item -> assertThat(item, hasConsistentTemporaryLoanType()));

    firstPageItems.forEach(ItemsApiTest::hasStatus);
    firstPageItems.forEach(item -> assertThat(item, hasConsistentPermanentLocation()));
    firstPageItems.forEach(item -> assertThat(item, hasConsistentTemporaryLocation()));
    firstPageItems.forEach(item -> assertThat(item,
      hasCallNumbers(CALL_NUMBER, CALL_NUMBER_SUFFIX, CALL_NUMBER_PREFIX, CALL_NUMBER_TYPE_ID)));

    secondPageItems.forEach(item -> assertThat(item, hasConsistentMaterialType()));
    secondPageItems.forEach(item -> assertThat(item, hasConsistentPermanentLoanType()));
    secondPageItems.forEach(item -> assertThat(item, hasConsistentTemporaryLoanType()));
    secondPageItems.forEach(ItemsApiTest::hasStatus);
    secondPageItems.forEach(item -> assertThat(item, hasConsistentPermanentLocation()));
    secondPageItems.forEach(item -> assertThat(item, hasConsistentTemporaryLocation()));
    secondPageItems.forEach(item -> assertThat(item,
      hasCallNumbers(CALL_NUMBER, CALL_NUMBER_SUFFIX, CALL_NUMBER_PREFIX, CALL_NUMBER_TYPE_ID)));
  }

  @Test
  @SneakyThrows
  void canGetAllItemsWithDifferentTemporaryLoanType() {
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

    final var getAllCompleted = okapiClient.get(ApiRoot.items());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.statusCode(), is(200));

    List<JsonObject> items = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("items"));

    assertThat(items.size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));

    assertThat(items.stream()
        .filter(item -> Strings.CS.equals(item.getString("barcode"), "645398607547"))
        .findFirst().orElse(new JsonObject()).getJsonObject("permanentLoanType").getString("id"),
      is(ApiTestSuite.getCanCirculateLoanType()));

    assertThat(items.stream()
      .filter(item -> Strings.CS.equals(item.getString("barcode"), "645398607547"))
      .findFirst().orElse(new JsonObject()).containsKey("temporaryLoanType"), is(false));

    assertThat(items.stream()
        .filter(item -> Strings.CS.equals(item.getString("barcode"), "175848607547"))
        .findFirst().orElse(new JsonObject()).getJsonObject("permanentLoanType").getString("id"),
      is(ApiTestSuite.getCanCirculateLoanType()));

    assertThat(items.stream()
        .filter(item -> Strings.CS.equals(item.getString("barcode"), "175848607547"))
        .findFirst().orElse(new JsonObject()).getJsonObject("temporaryLoanType").getString("id"),
      is(ApiTestSuite.getCourseReserveLoanType()));

    items.forEach(item -> assertThat(item, hasConsistentPermanentLoanType()));
    items.forEach(item -> assertThat(item, hasConsistentTemporaryLoanType()));
    items.forEach(item -> assertThat(item, hasConsistentPermanentLocation()));
    items.forEach(item -> assertThat(item, hasConsistentTemporaryLocation()));
  }

  @Test
  @SneakyThrows
  void pageParametersMustBeNumeric() {
    final var getPagedCompleted = okapiClient.get(ApiRoot.items("limit=&offset="));

    Response getPagedResponse = getPagedCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getPagedResponse.statusCode(), is(400));
    assertThat(getPagedResponse.body(),
      is("limit and offset must be numeric when supplied"));
  }

  @Test
  @SneakyThrows
  void cannotSearchForItemsByTitle() {
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

    final var searchGetCompleted
      = okapiClient.get(ApiRoot.items("query=title=*Small%20Angry*"));

    Response searchGetResponse = searchGetCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(searchGetResponse.statusCode(), is(200));

    List<JsonObject> items = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("items"));

    assertThat(items.size(), is(0));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  @SneakyThrows
  void canSearchForItemsByPostRetrieve() {
    List<String> itemIdz = new ArrayList<>();
    int numOfItemsToCreate = 5;
    for (int i = 1; i <= numOfItemsToCreate; i++) {
      JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

      UUID smallAngryHoldingId = holdingsStorageClient.create(
          new HoldingRequestBuilder()
            .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
        .getId();

      String itemId = itemsClient.create(new ItemRequestBuilder()
        .forHolding(smallAngryHoldingId)
        .book()
        .canCirculate()
        .withBarcode("1234567890" + i)).getId().toString();
      itemIdz.add(itemId);
    }

    String idzWithOrDelimiter = "id==(" + String.join(" or ", itemIdz) + ")";
    CQLQueryRequestDto cqlQueryRequestDto = new CQLQueryRequestDto();
    cqlQueryRequestDto.setQuery(idzWithOrDelimiter);
    cqlQueryRequestDto.setLimit(2000);
    final var postCompleted = okapiClient.post(ApiRoot.itemsRetrieve(), JsonObject.mapFrom(cqlQueryRequestDto));

    Response retrievePostResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(retrievePostResponse.statusCode(), is(200));

    List<JsonObject> items = JsonArrayHelper.toList(
      retrievePostResponse.getJson().getJsonArray("items"));

    assertThat(items.size(), is(numOfItemsToCreate));
    assertThat(retrievePostResponse.getJson().getInteger("totalRecords"), is(numOfItemsToCreate));
  }

  @Test
  @SneakyThrows
  void canPageAllIRetrieveItemsViaPost() {

    int numOfItemsToCreate = 5;
    for (int i = 1; i <= numOfItemsToCreate; i++) {
      JsonObject smallAngryInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

      UUID smallAngryHoldingId = holdingsStorageClient.create(
          new HoldingRequestBuilder()
            .forInstance(UUID.fromString(smallAngryInstance.getString("id"))))
        .getId();

      itemsClient.create(new ItemRequestBuilder()
        .forHolding(smallAngryHoldingId)
        .book()
        .canCirculate()
        .withBarcode("0987563431" + i));
    }

    CQLQueryRequestDto cqlQueryRequestDto = new CQLQueryRequestDto();
    cqlQueryRequestDto.setLimit(3);
    final var retrievePostCompletedFirstPage =
      okapiClient.post(ApiRoot.itemsRetrieve(), JsonObject.mapFrom(cqlQueryRequestDto));

    cqlQueryRequestDto.setLimit(3);
    cqlQueryRequestDto.setOffset(3);
    final var retrievePostCompletedSecondPage = okapiClient.post(ApiRoot.itemsRetrieve(),
      JsonObject.mapFrom(cqlQueryRequestDto));

    Response retrievePostPageResponseFirst = retrievePostCompletedFirstPage.toCompletableFuture().get(5, SECONDS);
    Response retrievePostPageResponseSecond = retrievePostCompletedSecondPage.toCompletableFuture().get(5, SECONDS);

    assertThat(retrievePostPageResponseFirst.statusCode(), is(200));
    assertThat(retrievePostPageResponseSecond.statusCode(), is(200));

    List<JsonObject> firstPageItems = JsonArrayHelper.toList(
      retrievePostPageResponseFirst.getJson().getJsonArray("items"));
    List<JsonObject> secondPageItems = JsonArrayHelper.toList(
      retrievePostPageResponseSecond.getJson().getJsonArray("items"));

    assertThat(firstPageItems.size(), is(3));
    assertThat(retrievePostPageResponseFirst.getJson().getInteger("totalRecords"), is(numOfItemsToCreate));

    assertThat(secondPageItems.size(), is(2));
    assertThat(retrievePostPageResponseSecond.getJson().getInteger("totalRecords"), is(numOfItemsToCreate));
  }

  @Test
  @SneakyThrows
  void cannotCreateSecondItemWithSameBarcode() {

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

    final var createItemCompleted = okapiClient.post(ApiRoot.items(), sameBarcodeItemRequest);

    Response sameBarcodeCreateResponse = createItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(sameBarcodeCreateResponse.statusCode(), is(400));
    assertThat(sameBarcodeCreateResponse.body(),
      is("Barcode must be unique, 645398607547 is already assigned to another item"));
  }

  @Test
  @SneakyThrows
  void cannotUpdateItemToSameBarcodeAsExistingItem() {
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

    URL nodItemLocation = new URI(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId())).toURL();

    final var putItemCompleted = okapiClient.put(nodItemLocation, changedNodItem);

    Response putItemResponse = putItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(putItemResponse.statusCode(), is(400));
    assertThat(putItemResponse.body(),
      is("Barcode must be unique, 645398607547 is already assigned to another item"));
  }

  @Test
  @SneakyThrows
  void canChangeBarcodeForExistingItem() {

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

    URL nodItemLocation = new URI(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId())).toURL();

    final var putItemCompleted = okapiClient.put(nodItemLocation, changedNodItem);

    Response putItemResponse = putItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(putItemResponse.statusCode(), is(204));

    final var getItemCompleted = okapiClient.get(nodItemLocation);

    Response getItemResponse = getItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getItemResponse.statusCode(), is(200));
    assertThat(getItemResponse.getJson().getString("barcode"), is("645398607547"));
  }

  @Test
  @SneakyThrows
  void canRemoveBarcodeFromAnExistingItem() {

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

    URL nodItemLocation = new URI(String.format("%s/%s",
      ApiRoot.items(), nodItemResponse.getId())).toURL();

    final var putItemCompleted = okapiClient.put(nodItemLocation, changedNodItem);

    Response putItemResponse = putItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(putItemResponse.statusCode(), is(204));

    final var getItemCompleted = okapiClient.get(nodItemLocation);

    Response getItemResponse = getItemCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getItemResponse.statusCode(), is(200));
    assertThat(getItemResponse.getJson().containsKey("barcode"), is(false));
  }

  @Test
  @SneakyThrows
  void canCreateAnItemWithACirculationNote() {

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
  }

  @Test
  @SneakyThrows
  void canCreateAnItemWithACirculationNoteWithoutSourceField() {

    JsonObject createdInstance = createInstance(smallAngryPlanet(UUID.randomUUID()));

    UUID holdingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(UUID.fromString(createdInstance.getString("id"))))
      .getId();

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
    checkInNote.remove("source");

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
    assertThat(checkInNote.getString(DATE_KEY), withinSecondsAfter(Seconds.seconds(2), requestMade));
  }

  @Test
  @SneakyThrows
  void canUpdateAnItemWithExistingCirculationNote() {

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

    assertThat(getResponse.statusCode(), is(200));
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
  }

  @Test
  @SneakyThrows
  void canPopulateLocationProperties() {
    UUID itemId = UUID.randomUUID();

    JsonObject newItemRequest = newDvdItemAtReadingRoom(itemId.toString());

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
  @SneakyThrows
  void canSearchItemsByLocation() {
    JsonObject readingRoomItem = newDvdItemAtReadingRoom(UUID.randomUUID().toString());

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
  @SneakyThrows
  void itemHasLastCheckInPropertiesWhenTheyAreSet() {

    JsonObject readingRoomItem = newDvdItemAtReadingRoom(UUID.randomUUID().toString());

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
  @SneakyThrows
  void itemHasNoLastCheckInPropertiesWhenNotSet() {
    JsonObject readingRoomItem = newDvdItemAtReadingRoom(UUID.randomUUID().toString());

    itemsStorageClient.create(readingRoomItem);

    JsonObject readingRoomItems = findItems("effectiveLocationId=" + getReadingRoomLocation());

    JsonObject actualItem = readingRoomItems.getJsonArray("items").getJsonObject(0);
    assertThat(actualItem, is(notNullValue()));
    assertThat(actualItem.getJsonObject(LAST_CHECK_IN_FIELD), is(nullValue()));
  }

  @Test
  @SneakyThrows
  void cannotChangeHRID() {

    UUID holdingId = createInstanceAndHolding();

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

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);

    assertThat(updateResponse,
      hasValidationError("HRID can not be updated", "hrid", "updatedHrid")
    );

    JsonObject existingItem = itemsClient.getById(postResponse.getId()).getJson();
    assertThat(existingItem, is(createdItem));
  }

  @Test
  @SneakyThrows
  void cannotRemoveHRID() {

    UUID holdingId = createInstanceAndHolding();

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

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);

    assertThat(updateResponse,
      hasValidationError("HRID can not be updated", "hrid", null)
    );

    JsonObject existingItem = itemsClient.getById(postResponse.getId()).getJson();
    assertThat(existingItem, is(createdItem));
  }

  @Test
  @SneakyThrows
  void cannotCreateItemWithUnrecognisedStatusName() {

    JsonObject itemWithUnrecognizedStatus = new ItemRequestBuilder()
      .forHolding(UUID.randomUUID())
      .withBarcode("645398607547")
      .temporarilyInReadingRoom()
      .create()
      .put("status", new JsonObject().put("name", "Unrecognized name"));

    final var postCompleted = okapiClient.post(items(""), itemWithUnrecognizedStatus);

    Response response = postCompleted.toCompletableFuture().get(5, SECONDS);
    assertThat(response, hasValidationError(
      "Undefined status specified",
      "status.name",
      "Unrecognized name"
    ));
  }

  @Test
  @SneakyThrows
  void cannotUpdateItemWithUnrecognisedStatusName() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy()
      .put("status", new JsonObject().put("name", "Unrecognized name"));

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);
    assertThat(updateResponse, hasValidationError(
      "Undefined status specified",
      "status.name",
      "Unrecognized name"
    ));
  }

  @Test
  @SneakyThrows
  void cannotRemoveStatusFromItem() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy();
    updatedItem.remove("status");

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);
    assertThat(updateResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  @SneakyThrows
  void cannotRemoveStatusNameFromItem() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource postResponse = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .temporarilyInReadingRoom());

    JsonObject createdItem = postResponse.getJson();
    assertThat(createdItem.getJsonObject("status").getString("name"), is("Available"));

    JsonObject updatedItem = createdItem.copy()
      .put("status", new JsonObject());

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);
    assertThat(updateResponse,
      hasValidationError("Status is a required field", "status", null)
    );
  }

  @Test
  @SneakyThrows
  void statusDatePropertyPresentOnStatusUpdated() {
    UUID holdingId = createInstanceAndHolding();

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

  @Test
  @SneakyThrows
  void cannotMarkClaimedReturnedItemAsMissing() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus("Claimed returned")
      .withBarcode("645398607547")
      .canCirculate());

    String itemStatus = createdItem.getJson().getJsonObject("status")
      .getString("name");
    assertThat(itemStatus, is("Claimed returned"));

    JsonObject updatedItem = createdItem.getJson().copy()
      .put("status", new JsonObject().put("name", "Missing"));

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);
    assertThat(updateResponse,
      hasValidationError("Claimed returned item cannot be marked as missing",
        "status.name", "Missing")
    );
  }

  @Test
  @SneakyThrows
  void canMarkClaimedReturnedItemAsAvailable() {
    UUID holdingId = createInstanceAndHolding();

    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withStatus("Claimed returned")
      .withBarcode("645398607547")
      .canCirculate());

    String itemStatus = createdItem.getJson().getJsonObject("status")
      .getString("name");
    assertThat(itemStatus, is("Claimed returned"));

    JsonObject updatedItem = createdItem.getJson().copy()
      .put("status", new JsonObject().put("name", "Available"));

    Response updateResponse = itemsClient.attemptToReplace(UUID.fromString(updatedItem.getString("id")), updatedItem);
    assertThat(updateResponse.statusCode(), is(204));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "Available",
    "Awaiting pickup",
    "Awaiting delivery",
    "Checked out",
    "In process",
    "In transit",
    "Missing",
    "On order",
    "Paged",
    "Declared lost",
    "Order closed",
    "Claimed returned",
    "Withdrawn",
    "Lost and paid",
    "Aged to lost"
  })
  void canCreateItemsWithAllStatuses(String itemStatus) {
    final UUID holdingsId = createInstanceAndHolding();

    final IndividualResource createResponse = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(holdingsId)
        .canCirculate()
        .withStatus(itemStatus));

    assertThat(createResponse.getJson().getJsonObject("status").getString("name"),
      is(itemStatus));
  }

  @Test
  @SneakyThrows
  void canCreateAndUpdateItemWithCompleteAdditionalCallNumbers() {
    UUID holdingId = createInstanceAndHolding();
    JsonArray additionalCallNumbers = new JsonArray();
    final String callNumber = "123";
    final String prefix = "A";
    final String suffix = "Z";
    final String typeId = CALL_NUMBER_TYPE_ID;
    additionalCallNumbers
      .add(new EffectiveCallNumberComponents(callNumber, prefix, suffix, typeId));
    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withAdditionalCallNumbers(additionalCallNumbers));

    final JsonObject itemLevelCallNumbers = createdItem.getJson().getJsonArray("additionalCallNumbers")
      .getJsonObject(0);

    final String additionalCallNumber = itemLevelCallNumbers.getString("callNumber");
    final String additionalCallNumberPrefix = itemLevelCallNumbers.getString("prefix");
    final String additionalCallNumberSuffix = itemLevelCallNumbers.getString("suffix");
    final String additionalCallNumberTypeId = itemLevelCallNumbers.getString("typeId");
    assertThat(additionalCallNumber, is(callNumber));
    assertThat(additionalCallNumberPrefix, is(prefix));
    assertThat(additionalCallNumberSuffix, is(suffix));
    assertThat(additionalCallNumberTypeId, is(typeId));

    JsonArray updatedAdditionalCallNumbers = new JsonArray();
    final String newCallNumber = "321";
    updatedAdditionalCallNumbers
      .add(new EffectiveCallNumberComponents(newCallNumber, prefix, suffix, typeId));
    JsonObject itemToUpdate = createdItem.getJson().copy()
      .put("additionalCallNumbers", updatedAdditionalCallNumbers);

    itemsClient.replace(createdItem.getId(), itemToUpdate);
    final JsonObject updatedItem = itemsClient.getById(createdItem.getId()).getJson();

    final String updatedAdditionalCallNumber = updatedItem.getJsonArray("additionalCallNumbers").getJsonObject(0)
      .getString("callNumber");
    assertThat(updatedAdditionalCallNumber, is(newCallNumber));
  }

  @Test
  @SneakyThrows
  void canCreateItemWithMinimalAdditionalCallNumbers() {
    UUID holdingId = createInstanceAndHolding();
    JsonArray additionalCallNumbers = new JsonArray();
    final String callNumber = "123";
    additionalCallNumbers
      .add(new EffectiveCallNumberComponents(callNumber, null, null, null));
    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withAdditionalCallNumbers(additionalCallNumbers));

    final JsonObject itemLevelCallNumbers = createdItem.getJson().getJsonArray("additionalCallNumbers")
      .getJsonObject(0);

    final String additionalCallNumber = itemLevelCallNumbers.getString("callNumber");
    assertThat(additionalCallNumber, is(callNumber));
  }

  @Test
  @SneakyThrows
  void canDeleteAdditionalCallNumbers() {
    UUID holdingId = createInstanceAndHolding();
    JsonArray additionalCallNumbers = new JsonArray();
    final String callNumber = "123";
    final String prefix = "A";
    final String suffix = "Z";
    final String typeId = CALL_NUMBER_TYPE_ID;
    additionalCallNumbers
      .add(new EffectiveCallNumberComponents(callNumber, prefix, suffix, typeId));
    IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingId)
      .withAdditionalCallNumbers(additionalCallNumbers));

    final JsonObject itemLevelCallNumbers = createdItem.getJson().getJsonArray("additionalCallNumbers")
      .getJsonObject(0);

    final String additionalCallNumber = itemLevelCallNumbers.getString("callNumber");
    final String additionalCallNumberPrefix = itemLevelCallNumbers.getString("prefix");
    final String additionalCallNumberSuffix = itemLevelCallNumbers.getString("suffix");
    final String additionalCallNumberTypeId = itemLevelCallNumbers.getString("typeId");
    assertThat(additionalCallNumber, is(callNumber));
    assertThat(additionalCallNumberPrefix, is(prefix));
    assertThat(additionalCallNumberSuffix, is(suffix));
    assertThat(additionalCallNumberTypeId, is(typeId));

    JsonObject itemToUpdate = createdItem.getJson().copy();
    itemToUpdate.remove("additionalCallNumbers");

    Response response = itemsClient.attemptToReplace(createdItem.getId(), itemToUpdate);
    assertThat(response.statusCode(), is(204));
  }

  @Test
  @SneakyThrows
  void canPatchExistingItem() {
    UUID transitDestinationServicePointIdForCreate = UUID.randomUUID();
    UUID transitDestinationServicePointIdForUpdate = UUID.randomUUID();
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject lastCheckIn = defaultLastCheckIn();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .withCheckInNote()
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));
    assertThat(newItemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForCreate.toString()));

    var patchRequest = new JsonObject()
      .put("id", itemId)
      .put("barcode", newItemRequest.getString("barcode"))
      .put("status", new JsonObject().put("name", "Checked out"))
      .put("copyNumber", "updatedCp")
      .put(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY, transitDestinationServicePointIdForUpdate)
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    itemsClient.patch(itemId, patchRequest);

    Response getResponse = itemsClient.getById(itemId);

    assertThat(getResponse.statusCode(), is(200));
    JsonObject updatedItem = getResponse.getJson();

    assertThat(getTags(updatedItem), hasItem(""));
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

    assertThat(updatedItem.getString("copyNumber"), is("updatedCp"));
    assertThat(updatedItem.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForUpdate.toString()));
    assertThat(updatedItem.getJsonArray("circulationNotes").size(), is(1));
  }

  @Test
  @SneakyThrows
  void cannotPatchItemThatDoesNotExist() {
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject patchItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .canCirculate()
      .temporarilyInReadingRoom()
      .create();

    final var patchCompleted = okapiClient.patch(
      String.format("%s/%s", ApiRoot.items(), patchItemRequest.getString("id")),
      patchItemRequest);

    Response patchResponse = patchCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(patchResponse.statusCode(), is(404));
  }

  @Test
  @SneakyThrows
  void cannotPatchItemIfHridWasChanged() {
    UUID transitDestinationServicePointIdForCreate = UUID.randomUUID();
    UUID transitDestinationServicePointIdForUpdate = UUID.randomUUID();
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject lastCheckIn = defaultLastCheckIn();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));
    assertThat(newItemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForCreate.toString()));

    var patchRequest = new JsonObject()
      .put("id", itemId)
      .put(HRID_KEY, "new_hrid")
      .put(BARCODE_KEY, "645398607547")
      .put(STATUS_KEY, new JsonObject().put("name", "Checked out"))
      .put("copyNumber", "updatedCp")
      .put(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY, transitDestinationServicePointIdForUpdate)
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    final var patchCompleted = okapiClient.patch(
      String.format("%s/%s", ApiRoot.items(), patchRequest.getString("id")),
      patchRequest);

    Response patchResponse = patchCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(patchResponse.statusCode(), is(422));
  }

  @Test
  @SneakyThrows
  void cannotPatchItemIfBarcodeExists() {
    UUID transitDestinationServicePointIdForCreate = UUID.randomUUID();
    UUID transitDestinationServicePointIdForUpdate = UUID.randomUUID();
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject lastCheckIn = defaultLastCheckIn();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(UUID.randomUUID())
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("existing_barcode")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    itemsClient.create(newItemRequest).getJson();

    newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));
    assertThat(newItemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForCreate.toString()));

    var patchRequest = new JsonObject()
      .put("id", itemId)
      .put(BARCODE_KEY, "existing_barcode")
      .put(HRID_KEY, newItemRequest.getString(HRID_KEY))
      .put(STATUS_KEY, new JsonObject().put("name", "Checked out"))
      .put("copyNumber", "updatedCp")
      .put(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY,
        transitDestinationServicePointIdForUpdate)
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    final var patchCompleted = okapiClient.patch(
      String.format("%s/%s", ApiRoot.items(), patchRequest.getString("id")),
      patchRequest);

    Response patchResponse = patchCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(patchResponse.statusCode(), is(400));
  }

  @Test
  @SneakyThrows
  void cannotPatchItemWithIncorrectStatus() {
    UUID transitDestinationServicePointIdForCreate = UUID.randomUUID();
    UUID transitDestinationServicePointIdForUpdate = UUID.randomUUID();
    UUID holdingId = createInstanceAndHolding();
    UUID itemId = UUID.randomUUID();

    JsonObject lastCheckIn = defaultLastCheckIn();

    JsonObject newItemRequest = new ItemRequestBuilder()
      .withId(itemId)
      .forHolding(holdingId)
      .withInTransitDestinationServicePointId(transitDestinationServicePointIdForCreate)
      .withBarcode("645398607547")
      .canCirculate()
      .temporarilyInReadingRoom()
      .withTagList(new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray().add("test-tag")))
      .withLastCheckIn(lastCheckIn)
      .withCopyNumber("cp")
      .create();

    newItemRequest = itemsClient.create(newItemRequest).getJson();

    assertThat(newItemRequest.getString("copyNumber"), is("cp"));
    assertThat(newItemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      is(transitDestinationServicePointIdForCreate.toString()));

    var patchRequest = new JsonObject()
      .put("id", itemId)
      .put("status", new JsonObject().put("name", "Invalid status"))
      .put("copyNumber", "updatedCp")
      .put(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY, transitDestinationServicePointIdForUpdate)
      .put("tags", new JsonObject().put("tagList", new JsonArray().add("")));

    final var patchCompleted = okapiClient.patch(
      String.format("%s/%s", ApiRoot.items(), patchRequest.getString("id")),
      patchRequest);

    Response patchResponse = patchCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(patchResponse.statusCode(), is(422));
  }

  @Test
  void titleIsBasedUponInstance() {
    UUID instanceId = instancesClient.create(
      InstanceRequestFixture.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestFixture.basedUponSmallAngryPlanet()
        .forHolding(holdingId));

    JsonObject createdItem = response.getJson();

    assertThat("has title from instance",
      createdItem.getString("title"), Is.is("The Long Way to a Small, Angry Planet"));
  }

  @Test
  @SneakyThrows
  void titlesComeFromInstancesForMultipleItems() {
    UUID firstInstanceId = instancesClient.create(
      InstanceRequestFixture.smallAngryPlanet()).getId();

    UUID firstHoldingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(firstInstanceId))
      .getId();

    UUID firstItemId = itemsClient.create(
        ItemRequestFixture.basedUponSmallAngryPlanet()
          .forHolding(firstHoldingId))
      .getId();

    UUID secondInstanceId = instancesClient.create(
      InstanceRequestFixture.temeraire()).getId();

    UUID secondHoldingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(secondInstanceId))
      .getId();

    UUID secondItemId = itemsClient.create(
        ItemRequestFixture.basedUponTemeraire()
          .forHolding(secondHoldingId))
      .getId();

    List<JsonObject> fetchedItemsResponse = itemsClient.getAll();

    assertThat(fetchedItemsResponse.size(), Is.is(2));

    JsonObject firstFetchedItem = getRecordById(
      fetchedItemsResponse, firstItemId).orElse(new JsonObject());

    assertThat("has title from instance",
      firstFetchedItem.getString("title"), Is.is("The Long Way to a Small, Angry Planet"));

    JsonObject secondFetchedItem = getRecordById(
      fetchedItemsResponse, secondItemId).orElse(new JsonObject());

    assertThat("has title from instance",
      secondFetchedItem.getString("title"), Is.is("Temeraire"));
  }

  @Test
  void readOnlyTitleIsNotStoredWhenCreated() {
    UUID instanceId = instancesClient.create(
      InstanceRequestFixture.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestFixture.basedUponSmallAngryPlanet()
        .withReadOnlyTitle("Should be discarded")
        .forHolding(holdingId));

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("title should not be stored",
      storedItemResponse.getJson().containsKey("title"), Is.is(false));
  }

  @Test
  @SneakyThrows
  void readOnlyTitleIsNotStoredWhenUpdated() {
    UUID instanceId = instancesClient.create(
      InstanceRequestFixture.smallAngryPlanet()).getId();

    UUID holdingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId))
      .getId();

    IndividualResource response = itemsClient.create(
      ItemRequestFixture.basedUponSmallAngryPlanet()
        .withReadOnlyTitle("Should be discarded")
        .forHolding(holdingId));

    itemsClient.replace(response.getId(), response.getJson());

    Response storedItemResponse = itemsStorageClient.getById(response.getId());

    assertThat("title should not be stored",
      storedItemResponse.getJson().containsKey("title"), Is.is(false));
  }

  private static void hasStatus(JsonObject item) {
    assertThat(item.containsKey("status"), is(true));
    assertThat(item.getJsonObject("status").containsKey("name"), is(true));
  }

  private JsonObject newDvdItemAtReadingRoom(String id) {
    return new JsonObject()
      .put("id", id)
      .put("status", new JsonObject().put("name", "Available"))
      .put("holdingsRecordId", createInstanceAndHolding().toString())
      .put("materialTypeId", getDvdMaterialType())
      .put("permanentLoanTypeId", getCanCirculateLoanType())
      .put("permanentLocationId", getMainLibraryLocation())
      .put("temporaryLocationId", getReadingRoomLocation());
  }

  private JsonObject defaultLastCheckIn() {
    return new JsonObject()
      .put("servicePointId", "7c5abc9f-f3d7-4856-b8d7-6712462ca007")
      .put("staffMemberId", "12115707-d7c8-54e7-8287-22e97f7250a4")
      .put("dateTime", "2020-01-02T13:02:46.000Z");
  }

  private JsonObject createInstance(JsonObject newInstanceRequest) {
    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  private UUID createInstanceAndHolding() {
    UUID instanceId = UUID.randomUUID();
    createInstance(smallAngryPlanet(instanceId));
    return holdingsStorageClient
      .create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }

  @SneakyThrows
  private JsonObject findItems(String searchQuery) {
    final var getCompleted = okapiClient.get(items("?query=") + urlEncode(searchQuery));

    return getCompleted.toCompletableFuture().get(5, SECONDS).getJson();
  }

  private List<String> getTags(JsonObject item) {
    return item.getJsonObject(Item.TAGS_KEY).getJsonArray(Item.TAG_LIST_KEY).stream()
      .map(Object::toString)
      .toList();
  }

  private static Optional<JsonObject> getRecordById(Collection<JsonObject> collection, UUID id) {
    return collection.stream()
      .filter(request -> Strings.CS.equals(request.getString("id"), id.toString()))
      .findFirst();
  }
}
