package api.instance;

import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.HttpStatus.HTTP_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.domain.instances.Dates.DATE1_KEY;
import static org.folio.inventory.domain.instances.Dates.DATE2_KEY;
import static org.folio.inventory.domain.instances.Dates.DATE_TYPE_ID_KEY;
import static org.folio.inventory.domain.instances.Dates.datesToJson;
import static org.folio.inventory.domain.instances.Instance.DATES_KEY;
import static org.folio.inventory.domain.instances.Instance.PRECEDING_TITLES_KEY;
import static org.folio.inventory.domain.instances.Instance.TAGS_KEY;
import static org.folio.inventory.domain.instances.Instance.TAG_LIST_KEY;
import static org.folio.inventory.resources.InstancesApi.SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static support.fixtures.InstanceFixture.leviathanWakes;
import static support.fixtures.InstanceFixture.marcInstanceWithDefaultBlockedFields;
import static support.fixtures.InstanceFixture.nod;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.fixtures.InstanceFixture.taoOfPooh;
import static support.fixtures.InstanceFixture.temeraire;
import static support.fixtures.InstanceFixture.treasureIslandInstance;
import static support.fixtures.InstanceFixture.uprooted;
import static support.matchers.ResponseMatchers.hasValidationError;

import api.ApiTestSuite;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.folio.HttpStatus;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.domain.instances.Dates;
import org.folio.inventory.domain.instances.Subject;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;
import support.FutureAssistance;
import support.InstanceApiClient;

public class InstancesApiTest extends ApiTests {

  private static final InventoryConfiguration CONFIG = new InventoryConfigurationImpl();
  private final String tagNameOne = "important";
  private final String tagNameTwo = "very important";
  private final String dateTypeId = "0750f52b-3bfc-458d-9307-e9afc8bcdffa";
  private final String date1 = "2014";
  private final String date2 = "2016";

  @AfterEach
  void disableFailureEmulation() throws Exception {
    instancesStorageClient.disableFailureEmulation();
    sourceRecordStorageClient.disableFailureEmulation();
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  @SneakyThrows
  void canCreateInstanceWithoutAnIdAndHrid() {
    String testNote = "this is a note";
    JsonArray adminNote = new JsonArray();
    adminNote.add(testNote);

    JsonObject newInstanceRequest = new JsonObject()
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
        .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
        .put("value", "9781473619777")))
      .put("contributors", new JsonArray().add(new JsonObject()
        .put("contributorNameTypeId", ApiTestSuite.getPersonalContributorNameType())
        .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("administrativeNotes", adminNote)
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType())
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add(tagNameOne)))
      .put(DATES_KEY, datesToJson(new Dates(dateTypeId, date1, date2)))
      .put("natureOfContentTermIds",
        new JsonArray(asList(
          ApiTestSuite.getAudiobookNatureOfContentTermId(),
          ApiTestSuite.getBibliographyNatureOfContentTermId()
        ))
      );

    Response postResponse = instancesClient.attemptToCreate(newInstanceRequest);

    String location = postResponse.location();

    assertThat(postResponse.statusCode(), is(201));
    assertThat(location, is(notNullValue()));
    assertThat(postResponse.body(), is(notNullValue()));

    JsonObject createdInstance = postResponse.getJson();

    assertThat(createdInstance.containsKey("administrativeNotes"), is(true));

    List<String> createdNotes = JsonArrayHelper.toListOfStrings(createdInstance.getJsonArray("administrativeNotes"));

    assertThat(createdNotes, contains(testNote));

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getTextInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstContributor = createdInstance.getJsonArray("contributors")
      .getJsonObject(0);

    assertThat(firstContributor.getString("contributorNameTypeId"),
      is(ApiTestSuite.getPersonalContributorNameType()));

    assertThat(firstContributor.getString("name"), is("Chambers, Becky"));

    assertTrue(createdInstance.containsKey(TAGS_KEY));
    final JsonObject tags = createdInstance.getJsonObject(TAGS_KEY);
    assertTrue(tags.containsKey(TAG_LIST_KEY));
    final JsonArray tagList = tags.getJsonArray(TAG_LIST_KEY);
    assertThat(tagList, hasItem(tagNameOne));

    JsonArray natureOfContentTermIds = createdInstance.getJsonArray("natureOfContentTermIds");
    assertThat(natureOfContentTermIds.size(), is(2));
    assertThat(natureOfContentTermIds, hasItem(ApiTestSuite.getAudiobookNatureOfContentTermId()));
    assertThat(natureOfContentTermIds, hasItem(ApiTestSuite.getBibliographyNatureOfContentTermId()));

    assertThat(createdInstance.getString("hrid"), notNullValue());

    var dates = createdInstance.getJsonObject(DATES_KEY);
    assertThat(dates.getString(DATE_TYPE_ID_KEY), is(dateTypeId));
    assertThat(dates.getString(DATE1_KEY), is(date1));
    assertThat(dates.getString(DATE2_KEY), is(date2));
  }

  @Test
  @SneakyThrows
  void canCreateAnInstanceWithAnIdAndHrid() {
    String instanceId = UUID.randomUUID().toString();
    final String hrid = "in777";

    JsonObject newInstanceRequest = new JsonObject()
      .put("id", instanceId)
      .put("hrid", hrid)
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
        .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
        .put("value", "9781473619777")))
      .put("contributors", new JsonArray().add(new JsonObject()
        .put("contributorNameTypeId", ApiTestSuite.getPersonalContributorNameType())
        .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("tags", null)
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    Response postResponse = instancesClient.attemptToCreate(newInstanceRequest);

    String location = postResponse.location();

    assertThat(postResponse.statusCode(), is(201));
    assertThat(location, is(notNullValue()));

    Response getResponse = FutureAssistance.getOnCompletion(okapiClient.get(location), 5, SECONDS);

    assertThat(getResponse.statusCode(), is(200));

    JsonObject createdInstance = getResponse.getJson();

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getTextInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstContributor = createdInstance.getJsonArray("contributors")
      .getJsonObject(0);

    assertThat(firstContributor.getString("contributorNameTypeId"),
      is(ApiTestSuite.getPersonalContributorNameType()));

    assertThat(firstContributor.getString("name"), is("Chambers, Becky"));

    assertThat(createdInstance.getString("hrid"), is(hrid));
    assertThat(createdInstance.getJsonObject("tags"), notNullValue());
  }

  @Test
  @SneakyThrows
  void canNotCreateInstanceIfDeletedIsTrueAndSuppressionFlagsAreFalse() {
    JsonObject newInstanceRequest = smallAngryPlanet(UUID.randomUUID())
      .put("staffSuppress", false)
      .put("discoverySuppress", false)
      .put("deleted", true);

    Response postResponse = instancesClient.attemptToCreate(newInstanceRequest);

    assertThat(postResponse.statusCode(), is(400));
    assertTrue(postResponse.hasBody());
    assertEquals(SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE, postResponse.body());
  }

  @Test
  @SneakyThrows
  void canCreateBatchOfInstances() {
    // Prepare request data
    String angryPlanetInstanceId = UUID.randomUUID().toString();
    JsonObject angryPlanetInstance = new JsonObject()
      .put("id", angryPlanetInstanceId)
      .put("title", "Long Way to a Small Angry Planet")
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType())
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add(tagNameOne).add(tagNameTwo)));

    String treasureIslandInstanceId = UUID.randomUUID().toString();
    JsonObject treasureIslandInstance = new JsonObject()
      .put("id", treasureIslandInstanceId)
      .put("title", "Treasure Island")
      .put("source", "MARC")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    JsonObject request = new JsonObject();
    request.put("instances", new JsonArray().add(angryPlanetInstance).add(treasureIslandInstance));
    request.put("totalRecords", 2);

    // Post collection of instances
    Response postResponse = instancesBatchClient.attemptToCreate(request);

    // Assertions
    assertThat(postResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    assertEquals(2, postResponse.getJson().getJsonArray("instances").size());
    assertEquals(0, postResponse.getJson().getJsonArray("errorMessages").size());
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(2));

    // Get and assert angryPlanetInstance
    Response getAngryPlanetInstanceResponse = instancesClient.getById(UUID.fromString(angryPlanetInstanceId));

    assertThat(getAngryPlanetInstanceResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject createdAngryPlanetInstance = getAngryPlanetInstanceResponse.getJson();
    assertEquals(createdAngryPlanetInstance.getString("id"), angryPlanetInstanceId);
    assertThat(createdAngryPlanetInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdAngryPlanetInstance.getString("source"), is("Local"));
    assertThat(createdAngryPlanetInstance.getString("instanceTypeId"), is(ApiTestSuite.getTextInstanceType()));

    assertTrue(createdAngryPlanetInstance.containsKey(TAGS_KEY));
    final JsonObject tags = createdAngryPlanetInstance.getJsonObject(TAGS_KEY);
    assertTrue(tags.containsKey(TAG_LIST_KEY));
    final JsonArray tagList = tags.getJsonArray(TAG_LIST_KEY);
    assertThat(tagList, hasItems(tagNameOne, tagNameTwo));

    // Get and assert treasureIslandInstance
    Response getTreasureIslandInstanceResponse = instancesClient.getById(UUID.fromString(treasureIslandInstanceId));

    assertThat(getTreasureIslandInstanceResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject createdTreasureIslandInstance = getTreasureIslandInstanceResponse.getJson();
    assertEquals(createdTreasureIslandInstance.getString("id"), treasureIslandInstanceId);
    assertThat(createdTreasureIslandInstance.getString("title"), is("Treasure Island"));
    assertThat(createdTreasureIslandInstance.getString("source"), is("MARC"));
    assertThat(createdTreasureIslandInstance.getString("instanceTypeId"), is(ApiTestSuite.getTextInstanceType()));
  }

  @Test
  @SneakyThrows
  void shouldReturnServerErrorIfOneInstancePostedWithoutTitle() {
    // Prepare request data
    String angryPlanetInstanceId = UUID.randomUUID().toString();
    JsonObject angryPlanetInstance = new JsonObject()
      .put("id", angryPlanetInstanceId)
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());
    JsonObject request = new JsonObject();
    request.put("instances", new JsonArray().add(angryPlanetInstance));
    request.put("total", 1);

    // Post instance
    Response postResponse = instancesBatchClient.attemptToCreate(request);

    // Assertions
    assertThat(postResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    assertEquals(0, postResponse.getJson().getJsonArray("instances").size());
    assertEquals(1, postResponse.getJson().getJsonArray("errorMessages").size());
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(0));
  }

  @Test
  @SneakyThrows
  void shouldReturnCreatedIfOneOfInstancesPostedWithoutTitle() {
    // Prepare request data
    String angryPlanetInstanceId = UUID.randomUUID().toString();
    JsonObject angryPlanetInstance = new JsonObject()
      .put("id", angryPlanetInstanceId)
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    String treasureIslandInstanceId = UUID.randomUUID().toString();
    JsonObject treasureIslandInstance = new JsonObject()
      .put("id", treasureIslandInstanceId)
      .put("title", "Treasure Island")
      .put("source", "MARC")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    JsonObject dealBreakerInstance = new JsonObject()
      .put("id", treasureIslandInstanceId)
      .put("title", "Deal Breaker")
      .put("source", "MARC")
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    JsonObject request = new JsonObject();
    request.put("instances",
      new JsonArray().add(angryPlanetInstance).add(treasureIslandInstance).add(dealBreakerInstance));
    request.put("totalRecords", 3);

    // Post instance
    Response postResponse = instancesBatchClient.attemptToCreate(request);

    // Assertions
    assertThat(postResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    assertEquals(2, postResponse.getJson().getJsonArray("instances").size());
    assertEquals(1, postResponse.getJson().getJsonArray("errorMessages").size());
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(2));
  }

  @Test
  @SneakyThrows
  void instanceTitleIsMandatory() {
    JsonObject newInstanceRequest = new JsonObject();

    Response postResponse = instancesClient.attemptToCreate(newInstanceRequest);

    assertThat(postResponse.statusCode(), is(400));
    assertThat(postResponse.contentType(), is(HttpHeaderValues.TEXT_PLAIN.toString()));
    assertThat(postResponse.location(), is(nullValue()));
    assertThat(postResponse.body(), is("Title must be provided for an instance"));
  }

  @Test
  @SneakyThrows
  void canUpdateAnExistingInstance() {
    UUID id = UUID.randomUUID();
    final var sourceId = "sourceId";
    final var typeId = "typeId";

    JsonObject smallAngryPlanet = smallAngryPlanet(id);
    smallAngryPlanet.put("natureOfContentTermIds",
      new JsonArray().add(ApiTestSuite.getBibliographyNatureOfContentTermId()));

    smallAngryPlanet.put(DATES_KEY, datesToJson(new Dates(null, date1, date2)));

    JsonObject newInstance = createInstance(smallAngryPlanet);

    JsonObject updateInstanceRequest = newInstance.copy()
      .put("title", "The Long Way to a Small, Angry Planet")
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add(tagNameTwo)))
      .put(DATES_KEY, datesToJson(new Dates(dateTypeId, date1, date2)))
      .put("natureOfContentTermIds",
        new JsonArray().add(ApiTestSuite.getAudiobookNatureOfContentTermId()))
      .put("subjects", new JsonArray().add(
        new Subject(null, null, sourceId, typeId)))
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", true);

    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(updateInstanceRequest.getString("id")), updateInstanceRequest);

    assertThat(putResponse.statusCode(), is(204));

    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));

    assertThat(getResponse.statusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();

    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is("The Long Way to a Small, Angry Planet"));
    assertThat(updatedInstance.getJsonArray("identifiers").size(), is(1));
    assertTrue(updatedInstance.getBoolean("deleted"));

    assertTrue(updatedInstance.containsKey(TAGS_KEY));
    final JsonObject tags = updatedInstance.getJsonObject(TAGS_KEY);
    assertTrue(tags.containsKey(TAG_LIST_KEY));
    final JsonArray tagList = tags.getJsonArray(TAG_LIST_KEY);
    assertThat(tagList, hasItem(tagNameTwo));

    JsonArray natureOfContentTermIds = updatedInstance.getJsonArray("natureOfContentTermIds");
    assertThat(natureOfContentTermIds.size(), is(1));
    assertThat(natureOfContentTermIds.getString(0), is(ApiTestSuite.getAudiobookNatureOfContentTermId()));

    var dates = updatedInstance.getJsonObject(DATES_KEY);
    assertThat(dates.getString(DATE_TYPE_ID_KEY), is(dateTypeId));
    assertThat(dates.getString(DATE1_KEY), is(date1));
    assertThat(dates.getString(DATE2_KEY), is(date2));

    var subjects = updatedInstance.getJsonArray("subjects");
    var subject = subjects.getJsonObject(0);
    assertThat(subjects.size(), is(1));
    assertThat(subject.getString(sourceId), is(sourceId));
    assertThat(subject.getString(typeId), is(typeId));
  }

  @Test
  @SneakyThrows
  void canUpdateAnExistingInstanceWithPrecedingSucceedingTitlesMarcSource() {
    UUID id = UUID.randomUUID();

    JsonObject smallAngryPlanet = smallAngryPlanet(id);
    smallAngryPlanet.put("natureOfContentTermIds",
      new JsonArray().add(ApiTestSuite.getBibliographyNatureOfContentTermId()));

    JsonArray precedingTitles = new JsonArray();
    precedingTitles.add(
      new JsonObject()
        .put("title", "Chilton's automotive industries")
        .put("identifiers", new JsonArray().add(
          new JsonObject()
            .put("identifierTypeId", "913300b2-03ed-469a-8179-c1092c991227")
            .put("value", "0273-656X"))
        ));
    smallAngryPlanet.put(PRECEDING_TITLES_KEY, precedingTitles);
    smallAngryPlanet.put("source", "MARC");

    JsonObject newInstance = createInstance(smallAngryPlanet);

    precedingTitles = new JsonArray();
    precedingTitles.add(
      new JsonObject()
        .put("title", "Chilton's automotive industries")
        .put("id", newInstance.getJsonArray("precedingTitles").getJsonObject(0).getString("id"))
        .put("identifiers", new JsonArray().add(
          new JsonObject()
            .put("identifierTypeId", "913300b2-03ed-469a-8179-c1092c991227")
            .put("value", "0273-656X"))
        ));

    JsonObject updateInstanceRequest = newInstance.copy()
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add(tagNameTwo)))
      .put(PRECEDING_TITLES_KEY, precedingTitles)
      .put("natureOfContentTermIds",
        new JsonArray().add(ApiTestSuite.getAudiobookNatureOfContentTermId()));

    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(updateInstanceRequest.getString("id")), updateInstanceRequest);

    assertThat(putResponse.statusCode(), is(204));
  }

  @Test
  @SneakyThrows
  void shouldReturnErrorIfFailedToUpdateSuppressFromDiscoveryInSrs() {
    UUID id = UUID.randomUUID();
    // Create new Instance (marked as deleted)
    JsonObject newInstance = createInstance(treasureIslandInstance(id)
      .put("deleted", true)
      .put("staffSuppress", true)
      .put("discoverySuppress", true));

    // Emulate failure on Source Record Storage side during updating suppression flags
    sourceRecordStorageClient.emulateFailure(500, PUT.name(), "Internal server error", "plain/text");

    JsonObject instanceForUpdate = newInstance.copy()
      .put("staffSuppress", true)
      .put("discoverySuppress", false)
      .put("deleted", false);

    // Put Instance for update
    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);
    assertThat(putResponse.statusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(putResponse.hasBody(), is(true));
    assertThat(putResponse.body(), is(
      format("Failed to update suppress from discovery flag for record in SRS. InstanceID: %s, StatusCode: 500", id)));

    // Get existing Instance
    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));
    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertFalse(getResponse.getJson().getBoolean("discoverySuppress"));
    assertFalse(getResponse.getJson().getBoolean("deleted"));
  }

  @Test
  @SneakyThrows
  void shouldReturnErrorIfFailedToUndeleteInSrs() {
    UUID id = UUID.randomUUID();
    // Create new Instance (marked as deleted)
    JsonObject newInstance = createInstance(treasureIslandInstance(id)
      .put("deleted", true)
      .put("staffSuppress", true)
      .put("discoverySuppress", true));

    // Emulate failure on Source Record Storage side during updating suppression flags
    sourceRecordStorageClient.emulateFailure(500, POST.name(), "Internal server error", "plain/text");

    JsonObject instanceForUpdate = newInstance.copy()
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", false);

    // Put Instance for update
    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);
    assertThat(putResponse.statusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(putResponse.hasBody(), is(true));
    assertThat(putResponse.body(), is(format("The instance wasn't undeleted in SRS. InstanceID: %s, SC: 500", id)));

    // Get existing Instance
    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));
    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));
    assertFalse(getResponse.getJson().getBoolean("deleted"));
  }

  @Test
  @SneakyThrows
  void canAddTagToExistingInstanceWithUnconnectedPrecedingSucceeding() {
    var smallAngryPlanet = smallAngryPlanet(UUID.randomUUID());

    var precedingTitles = new JsonArray();
    precedingTitles.add(
      new JsonObject()
        .put("title", "Chilton's automotive industries")
        .put("identifiers", new JsonArray().add(
          new JsonObject()
            .put("identifierTypeId", "913300b2-03ed-469a-8179-c1092c991227")
            .put("value", "0273-656X"))
        ));
    smallAngryPlanet.put(PRECEDING_TITLES_KEY, precedingTitles);
    smallAngryPlanet.put("source", "MARC");

    var newInstance = createInstance(smallAngryPlanet);

    precedingTitles = new JsonArray();
    precedingTitles.add(
      new JsonObject()
        .put("title", "Chilton's automotive industries")
        .put("id", newInstance.getJsonArray("precedingTitles").getJsonObject(0).getString("id"))
        .put(PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY, null)
        .put("identifiers", new JsonArray().add(
          new JsonObject()
            .put("identifierTypeId", "913300b2-03ed-469a-8179-c1092c991227")
            .put("value", "0273-656X"))
        ));
    var updateInstanceRequest = newInstance.copy()
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add("test")))
      .put(PRECEDING_TITLES_KEY, precedingTitles);

    var putResponse =
      instancesClient.attemptToReplace(UUID.fromString(updateInstanceRequest.getString("id")), updateInstanceRequest);

    assertThat(putResponse.statusCode(), is(204));
  }

  @Test
  @SneakyThrows
  void cannotUpdateAnInstanceThatDoesNotExist() {
    JsonObject updateInstanceRequest = smallAngryPlanet(UUID.randomUUID());

    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(updateInstanceRequest.getString("id")), updateInstanceRequest);

    assertThat(putResponse.statusCode(), is(404));
    assertThat(putResponse.body(), is("Instance not found"));
  }

  @Test
  @SneakyThrows
  void cannotUpdateAnInstanceWithOptimisticLockingFailure() {
    JsonObject instance = createInstance(smallAngryPlanet(ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE));

    Response putResponse = instancesClient.attemptToReplace(UUID.fromString(instance.getString("id")), instance);
    assertThat(putResponse.statusCode(), is(409));
    assertThat(putResponse.body(), is("Optimistic Locking"));
    assertThat(putResponse.contentType(), is(HttpHeaderValues.TEXT_PLAIN.toString()));
  }

  @Test
  @SneakyThrows
  void canUpdateAnExistingMarcInstanceIfNoChanges() {
    UUID id = UUID.randomUUID();
    // Create new Instance
    JsonObject newInstance = createInstance(treasureIslandInstance(id));
    JsonObject instanceForUpdate = newInstance.copy();
    // Put Instance for update
    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);
    assertThat(putResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
    // Get existing Instance
    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));

    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));

    JsonObject updatedInstance = getResponse.getJson();
    assertEquals(updatedInstance, newInstance);
  }

  @Test
  @SneakyThrows
  void canNotUpdateAnExistingMarcInstanceIfBlockedFieldsAreChanged() {
    UUID id = UUID.randomUUID();
    createInstance(treasureIslandInstance(id));
    JsonObject instanceForUpdate = marcInstanceWithDefaultBlockedFields(id);

    for (String field : CONFIG.getInstanceBlockedFields()) {
      // Put Instance for update
      Response putResponse =
        instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);

      assertThat(putResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
      assertThat(putResponse.getJson().getJsonArray("errors").size(), is(1));

      instanceForUpdate.remove(field);
    }
  }

  @Test
  @SneakyThrows
  void canNotUpdateAnExistingMarcInstanceIfBlockedFieldsAreChangedToNulls() {
    UUID id = UUID.randomUUID();
    JsonObject createInstanceRequest = treasureIslandInstance(id)
      .put("hrid", "test-hrid-0")
      .put("statusId", "test-statusId-0");
    // Create new Instance
    final JsonObject newInstance = createInstance(createInstanceRequest);

    JsonObject instanceForUpdate = treasureIslandInstance(id);
    // Put Instance for update
    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);

    assertThat(putResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
    assertNotNull(putResponse.getJson().getJsonArray("errors"));
    JsonArray errors = putResponse.getJson().getJsonArray("errors");
    assertThat(errors.size(), is(1));
    assertThat(errors.getJsonObject(0).getString("message"), is(
      "Instance is controlled by MARC record, these fields are blocked and can not be updated: "
      + "physicalDescriptions,notes,languages,precedingTitles,identifiers,instanceTypeId,"
      + "modeOfIssuanceId,subjects,dates,source,title,indexTitle,publicationFrequency,"
      + "electronicAccess,publicationRange,classifications,succeedingTitles,editions,hrid,series,"
      + "instanceFormatIds,publication,contributors,alternativeTitles"));

    // Get existing Instance
    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));

    assertThat(getResponse.statusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();
    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is(newInstance.getString("title")));
    assertThat(updatedInstance.getString("source"), is(newInstance.getString("source")));
    assertThat(updatedInstance.getString("hrid"), is(newInstance.getString("hrid")));
    assertThat(updatedInstance.getString("statusId"), is(newInstance.getString("statusId")));
  }

  @Test
  @SneakyThrows
  void canUpdateAnExistingMarcInstanceIfBlockedFieldsAreNotChanged() {
    UUID id = UUID.randomUUID();
    JsonObject createInstanceRequest = treasureIslandInstance(id)
      .put("sourceRecordFormat", "test-format-0"); // 'sourceRecordFormat' is non blocked field
    // Create new Instance
    JsonObject newInstance = createInstance(createInstanceRequest);

    JsonObject instanceForUpdate = newInstance.copy()
      .put("sourceRecordFormat", "test-format-1");
    // Put Instance for update
    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceForUpdate.getString("id")), instanceForUpdate);

    assertThat(putResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

    // Get existing Instance
    Response getResponse = instancesClient.getById(UUID.fromString(newInstance.getString("id")));

    assertThat(getResponse.statusCode(), is(HttpResponseStatus.OK.code()));

    JsonObject updatedInstance = getResponse.getJson();
    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is(newInstance.getString("title")));
    assertThat(updatedInstance.getString("source"), is(newInstance.getString("source")));
    assertThat(updatedInstance.getString("sourceRecordFormat"), is(instanceForUpdate.getString("sourceRecordFormat")));
  }

  @Test
  @SneakyThrows
  void canNotUpdateInstanceMarkedForDeletionIfSuppressionFlagsAreChangedToFalse() {
    JsonObject newInstanceRequest = smallAngryPlanet(UUID.randomUUID())
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", true);
    JsonObject newInstance = createInstance(newInstanceRequest);
    assertTrue(newInstance.getBoolean("deleted"));

    JsonObject updateInstanceRequest = newInstance.copy()
      .put("discoverySuppress", false)
      .put("staffSuppress", false);

    Response putResponse =
      instancesClient.attemptToReplace(UUID.fromString(updateInstanceRequest.getString("id")), updateInstanceRequest);

    assertThat(putResponse.statusCode(), is(400));
    assertTrue(putResponse.hasBody());
    assertEquals(SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE, putResponse.body());
  }

  @Test
  @SneakyThrows
  void canDeleteAllInstances() {
    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));

    Response deleteResponse = FutureAssistance.getOnCompletion(okapiClient.delete(
      ApiRoot.instances() + "?query=" + PercentCodec.encode("cql.allRecords=1")), 5, SECONDS);

    assertThat(deleteResponse.statusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    Response getAllResponse = FutureAssistance.getOnCompletion(okapiClient.get(ApiRoot.instances()), 5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  @SneakyThrows
  void canDeleteAnInstance() {
    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));

    JsonObject instanceToDelete = createInstance(leviathanWakes(UUID.randomUUID()));

    URL instanceToDeleteLocation = ApiRoot.instance(instanceToDelete.getString("id"));

    Response deleteResponse =
      FutureAssistance.getOnCompletion(okapiClient.delete(instanceToDeleteLocation), 5, SECONDS);

    assertThat(deleteResponse.statusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    Response getResponse = instancesClient.getById(UUID.fromString(instanceToDelete.getString("id")));

    assertThat(getResponse.statusCode(), is(404));

    Response getAllResponse = FutureAssistance.getOnCompletion(okapiClient.get(ApiRoot.instances()), 5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  @SneakyThrows
  void canSoftDeleteInstance() {
    UUID instanceId = UUID.randomUUID();
    final JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    JsonObject sourceRecord = new JsonObject().put("id", instanceId.toString());

    sourceRecordStorageClient.create(sourceRecord);
    Response getCreatedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getCreatedSourceRecordResponse.statusCode(), HttpStatus.HTTP_OK.toInt());
    assertEquals(instanceId.toString(), getCreatedSourceRecordResponse.getJson().getString("id"));

    URL softDeleteUrl = new URI(String.format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted")).toURL();

    Response deleteResponse = FutureAssistance.getOnCompletion(okapiClient.delete(softDeleteUrl), 5, SECONDS);

    assertThat(deleteResponse.statusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    Response getResponse = instancesClient.getById(UUID.fromString(instanceToDelete.getString("id")));

    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));
    assertTrue(getResponse.getJson().getBoolean("deleted"));

    Response getDeletedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getDeletedSourceRecordResponse.statusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());
  }

  @Test
  @SneakyThrows
  void canSoftDeleteInstanceIfSourceRecordNotFound() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    Response getSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getSourceRecordResponse.statusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());

    URL softDeleteUrl = new URI(format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted")).toURL();

    Response deleteResponse = FutureAssistance.getOnCompletion(okapiClient.delete(softDeleteUrl), 5, SECONDS);

    String expectedMessage = String.format(
      "MARC record was not set for deletion because it was not found by instance ID: %s", instanceId);
    assertThat(deleteResponse.statusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(deleteResponse.hasBody(), is(true));
    assertThat(deleteResponse.body(), is(expectedMessage));

    Response getResponse = instancesClient.getById(UUID.fromString(instanceToDelete.getString("id")));

    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));

    Response getDeletedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getDeletedSourceRecordResponse.statusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());
  }

  @Test
  @SneakyThrows
  void canSoftDeleteInstanceIfFailedToMarkSourceRecordAsDeleted() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    sourceRecordStorageClient.emulateFailure(500, DELETE.name(), "Internal server error", "plain/text");

    URL softDeleteUrl = URI.create(String.format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted")).toURL();

    Response deleteResponse = FutureAssistance.getOnCompletion(okapiClient.delete(softDeleteUrl), 5, SECONDS);

    assertThat(deleteResponse.statusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(deleteResponse.hasBody(), is(true));

    Response getResponse = instancesClient.getById(UUID.fromString(instanceToDelete.getString("id")));
    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));
  }

  @Test
  @SneakyThrows
  void canGetAllInstances() {
    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));

    Response getAllResponse = FutureAssistance.getOnCompletion(okapiClient.get(ApiRoot.instances()), 5, SECONDS);

    assertThat(getAllResponse.statusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(3));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(3));
  }

  @Test
  @SneakyThrows
  void canPageAllInstances() {
    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));
    createInstance(taoOfPooh(UUID.randomUUID()));

    Response firstPageResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.instances("limit=3")), 5, SECONDS);

    Response secondPageResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.instances("limit=3&offset=3")), 5, SECONDS);

    assertThat(firstPageResponse.statusCode(), is(200));
    assertThat(secondPageResponse.statusCode(), is(200));

    List<JsonObject> firstPageInstances = JsonArrayHelper.toList(
      firstPageResponse.getJson().getJsonArray("instances"));

    assertThat(firstPageInstances.size(), is(3));
    assertThat(firstPageResponse.getJson().getInteger("totalRecords"), is(5));

    List<JsonObject> secondPageInstances = JsonArrayHelper.toList(
      secondPageResponse.getJson().getJsonArray("instances"));

    assertThat(secondPageInstances.size(), is(2));
    assertThat(secondPageResponse.getJson().getInteger("totalRecords"), is(5));
  }

  @Test
  @SneakyThrows
  void pageParametersMustBeNumeric() {
    Response getPagedResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.instances("limit=&offset=")), 5, SECONDS);

    assertThat(getPagedResponse.statusCode(), is(400));
    assertThat(getPagedResponse.body(),
      is("limit and offset must be numeric when supplied"));
  }

  @Test
  @SneakyThrows
  void canSearchForInstancesByTitle() {
    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(uprooted(UUID.randomUUID()));

    Response searchGetResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.instances("query=title=Small%20Angry*")), 5, SECONDS);

    assertThat(searchGetResponse.statusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(1));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(1));
    assertThat(instances.getFirst().getString("title"), is("Long Way to a Small Angry Planet"));
  }

  @Test
  @SneakyThrows
  void cannotFindAnUnknownInstance() {
    Response getResponse = instancesClient.getById(UUID.randomUUID());

    assertThat(getResponse.statusCode(), is(404));
  }

  @Test
  @SneakyThrows
  void cannotChangeHrid() {
    UUID instanceId = UUID.randomUUID();
    JsonObject createdInstance = createInstance(smallAngryPlanet(instanceId));

    assertThat(createdInstance.getString("hrid"), notNullValue());

    JsonObject instanceToUpdate = createdInstance.copy()
      .put("title", "updatedTitle")
      .put("hrid", "updatedHrid");

    Response instanceUpdateResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceToUpdate.getString("id")), instanceToUpdate);

    String expectedMessage = String.format("HRID change detected: existing=%s, updated=%s",
      createdInstance.getString("hrid"), instanceToUpdate.getString("hrid"));

    assertThat(instanceUpdateResponse,
      hasValidationError(expectedMessage, "hrid", "updatedHrid"));

    JsonObject existingInstance = instancesClient.getById(instanceId).getJson();
    assertThat(existingInstance, is(createdInstance));
  }

  @Test
  @SneakyThrows
  void cannotRemoveHrid() {
    UUID instanceId = UUID.randomUUID();
    JsonObject createdInstance = createInstance(smallAngryPlanet(instanceId));

    assertThat(createdInstance.getString("hrid"), notNullValue());

    JsonObject instanceToUpdate = createdInstance.copy()
      .put("title", "updatedTitle");

    instanceToUpdate.remove("hrid");

    Response instanceUpdateResponse =
      instancesClient.attemptToReplace(UUID.fromString(instanceToUpdate.getString("id")), instanceToUpdate);

    String expectedMessage = String.format("HRID change detected: existing=%s, updated=%s",
      createdInstance.getString("hrid"), instanceToUpdate.getString("hrid"));

    assertThat(instanceUpdateResponse,
      hasValidationError(expectedMessage, "hrid", null));

    JsonObject existingInstance = instancesClient.getById(instanceId).getJson();
    assertThat(existingInstance, is(createdInstance));
  }

  @Test
  @SneakyThrows
  void canFrowardInstanceCreateFailureFromStorage() {
    final String expectedErrorMessage = "Instance-storage is temporary unavailable for create";

    instancesStorageClient.emulateFailure(500, POST.name(), expectedErrorMessage, "plain/text");

    final Response response = instancesClient.attemptToCreate(smallAngryPlanet(UUID.randomUUID()));

    assertThat(response.statusCode(), is(500));
    assertThat(response.body(), is(expectedErrorMessage));
  }

  @Test
  @SneakyThrows
  void canFrowardInstanceUpdateFailureFromStorage() {
    final String expectedErrorMessage = "Instance-storage is temporary unavailable for updates";

    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    instancesStorageClient.emulateFailure(500, PUT.name(), expectedErrorMessage, "plain/text");

    final Response updateResponse = instancesClient
      .attemptToReplace(instance.getId(), instance.getJson().copy()
        .put("subjects", new JsonArray().add("Small angry planet subject")));

    assertThat(updateResponse.statusCode(), is(500));
    assertThat(updateResponse.body(), is(expectedErrorMessage));
  }

  @Test
  @SneakyThrows
  void canFrowardInstanceCreateValidationErrorFromStorage() {
    final String expectedErrorMessage = "A note has exceeded the 32000 character limit.";

    instancesStorageClient.emulateFailure(422, POST.name(), expectedErrorMessage, "plain/text");

    final Response response = instancesClient.attemptToCreate(smallAngryPlanet(UUID.randomUUID()));

    assertThat(response.statusCode(), is(422));
    assertThat(response.body(), is(expectedErrorMessage));
  }

  @Test
  @SneakyThrows
  void canFrowardInstanceUpdateValidationErrorFromStorage() {
    final String expectedErrorMessage = "A note has exceeded the 32000 character limit.";

    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    instancesStorageClient.emulateFailure(422, PUT.name(), expectedErrorMessage, "plain/text");

    final Response updateResponse = instancesClient
      .attemptToReplace(instance.getId(), instance.getJson().copy()
        .put("subjects", new JsonArray().add("Small angry planet subject")));

    assertThat(updateResponse.statusCode(), is(422));
    assertThat(updateResponse.body(), is(expectedErrorMessage));
  }

  @Test
  @SneakyThrows
  void canPatchAnExistingInstance() {
    UUID id = UUID.randomUUID();
    final var sourceId = "sourceId";
    final var typeId = "typeId";

    JsonObject smallAngryPlanet = smallAngryPlanet(id);
    smallAngryPlanet.put("natureOfContentTermIds",
      new JsonArray().add(ApiTestSuite.getBibliographyNatureOfContentTermId()));

    smallAngryPlanet.put(DATES_KEY, datesToJson(new Dates(null, date1, date2)));

    JsonObject newInstance = createInstance(smallAngryPlanet);

    var instanceId = newInstance.getString("id");
    JsonObject patchRequest = new JsonObject()
      .put("id", newInstance.getString("id"))
      .put("title", "New title")
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add(tagNameTwo)))
      .put("subjects", new JsonArray().add(
        new Subject(null, null, sourceId, typeId)))
      .put(DATES_KEY, datesToJson(new Dates(dateTypeId, date1, date2)))
      .put("natureOfContentTermIds",
        new JsonArray().add(ApiTestSuite.getAudiobookNatureOfContentTermId()))
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", true);

    Response patchResponse = patchInstance(instanceId, patchRequest);

    assertThat(patchResponse.statusCode(), is(204));

    Response getResponse = instancesClient.getById(UUID.fromString(instanceId));

    assertThat(getResponse.statusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();

    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is("New title"));
    assertThat(updatedInstance.getJsonArray("identifiers").size(), is(1));
    assertTrue(updatedInstance.getBoolean("deleted"));

    assertTrue(updatedInstance.containsKey(TAGS_KEY));
    final JsonObject tags = updatedInstance.getJsonObject(TAGS_KEY);
    assertTrue(tags.containsKey(TAG_LIST_KEY));
    final JsonArray tagList = tags.getJsonArray(TAG_LIST_KEY);
    assertThat(tagList, hasItem(tagNameTwo));

    JsonArray natureOfContentTermIds = updatedInstance.getJsonArray("natureOfContentTermIds");
    assertThat(natureOfContentTermIds.size(), is(1));
    assertThat(natureOfContentTermIds.getString(0), is(ApiTestSuite.getAudiobookNatureOfContentTermId()));

    var dates = updatedInstance.getJsonObject(DATES_KEY);
    assertThat(dates.getString(DATE_TYPE_ID_KEY), is(dateTypeId));
    assertThat(dates.getString(DATE1_KEY), is(date1));
    assertThat(dates.getString(DATE2_KEY), is(date2));

    var subjects = updatedInstance.getJsonArray("subjects");
    var subject = subjects.getJsonObject(0);
    assertThat(subjects.size(), is(1));
    assertThat(subject.getString(sourceId), is(sourceId));
    assertThat(subject.getString(typeId), is(typeId));
  }

  @SneakyThrows
  private JsonObject createInstance(JsonObject newInstanceRequest) {
    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  @SneakyThrows
  private Response patchInstance(String id, JsonObject patchJson) {
    return instancesClient.attemptToPatch(UUID.fromString(id), patchJson);
  }
}
