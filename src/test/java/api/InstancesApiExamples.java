package api;

import static api.support.InstanceSamples.leviathanWakes;
import static api.support.InstanceSamples.marcInstanceWithDefaultBlockedFields;
import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static api.support.InstanceSamples.taoOfPooh;
import static api.support.InstanceSamples.temeraire;
import static api.support.InstanceSamples.treasureIslandInstance;
import static api.support.InstanceSamples.uprooted;
import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.HttpStatus.HTTP_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.domain.instances.Dates.DATE_TYPE_ID_KEY;
import static org.folio.inventory.domain.instances.Dates.DATE1_KEY;
import static org.folio.inventory.domain.instances.Dates.DATE2_KEY;
import static org.folio.inventory.domain.instances.Dates.datesToJson;
import static org.folio.inventory.domain.instances.Instance.DATES_KEY;
import static org.folio.inventory.domain.instances.Instance.PRECEDING_TITLES_KEY;
import static org.folio.inventory.domain.instances.Instance.TAGS_KEY;
import static org.folio.inventory.domain.instances.Instance.TAG_LIST_KEY;
import static org.folio.inventory.resources.Instances.FLAGS_SUPPRESSION_INCONSISTENCY_MESSAGE;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.HttpStatus;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.domain.instances.Dates;
import org.folio.inventory.domain.instances.Subject;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.ContentType;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import support.fakes.EndpointFailureDescriptor;

public class InstancesApiExamples extends ApiTests {
  private static final InventoryConfiguration config = new InventoryConfigurationImpl();
  private final String tagNameOne = "important";
  private final String tagNameTwo = "very important";
  private final String dateTypeId = "0750f52b-3bfc-458d-9307-e9afc8bcdffa";
  private final String date1 = "2014";
  private final String date2 = "2016";

  @After
  public void disableFailureEmulation() throws Exception {
    instancesStorageClient.disableFailureEmulation();
    sourceRecordStorageClient.disableFailureEmulation();
  }

  @Test
  public void canCreateInstanceWithoutAnIDAndHRID()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

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

    final var postCompleted = okapiClient
      .post(ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));
    assertThat(postResponse.getBody(), is(notNullValue()));

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
  public void canCreateAnInstanceWithAnIDAndHRID()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

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
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());

    final var postCompleted = okapiClient
      .post(ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));

    final var getCompleted = okapiClient.get(location);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

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
  }

  @Test
  public void canNotCreateInstanceIfDeletedIsTrueAndSuppressionFlagsAreFalse()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

    JsonObject newInstanceRequest = smallAngryPlanet(UUID.randomUUID())
      .put("staffSuppress", false)
      .put("discoverySuppress", false)
      .put("deleted", true);

    final var postCompleted = okapiClient
      .post(ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(postResponse.getStatusCode(), is(400));
    assertTrue(postResponse.hasBody());
    assertEquals(FLAGS_SUPPRESSION_INCONSISTENCY_MESSAGE, postResponse.getBody());
  }

  @Test
  public void canCreateBatchOfInstances() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
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
    final var postCompleted = okapiClient
      .post(ApiRoot.instancesBatch(), request);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    // Assertions
    assertThat(postResponse.getStatusCode(), is(HttpResponseStatus.CREATED.code()));
    assertEquals(postResponse.getJson().getJsonArray("instances").size(), 2);
    assertEquals(postResponse.getJson().getJsonArray("errorMessages").size(), 0);
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(2));

    // Get and assert angryPlanetInstance
    final var getAngryPlanetInstanceCompleted
      = okapiClient.get(String.format("%s/%s", ApiRoot.instances(), angryPlanetInstanceId));
    Response getAngryPlanetInstanceResponse
      = getAngryPlanetInstanceCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAngryPlanetInstanceResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
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
    final var getTreasureIslandInstanceCompleted
      = okapiClient.get(String.format("%s/%s", ApiRoot.instances(), treasureIslandInstanceId));
    Response getTreasureIslandInstanceResponse
      = getTreasureIslandInstanceCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getTreasureIslandInstanceResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));
    JsonObject createdTreasureIslandInstance = getTreasureIslandInstanceResponse.getJson();
    assertEquals(createdTreasureIslandInstance.getString("id"), treasureIslandInstanceId);
    assertThat(createdTreasureIslandInstance.getString("title"), is("Treasure Island"));
    assertThat(createdTreasureIslandInstance.getString("source"), is("MARC"));
    assertThat(createdTreasureIslandInstance.getString("instanceTypeId"), is(ApiTestSuite.getTextInstanceType()));
  }

  @Test
  public void shouldReturnServerErrorIfOneInstancePostedWithoutTitle() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
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
    final var postCompleted = okapiClient
      .post(ApiRoot.instancesBatch(), request);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    // Assertions
    assertThat(postResponse.getStatusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    assertEquals(postResponse.getJson().getJsonArray("instances").size(), 0);
    assertEquals(postResponse.getJson().getJsonArray("errorMessages").size(), 1);
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(0));
  }

  @Test
  public void shouldReturnCreatedIfOneOfInstancesPostedWithoutTitle() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
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
    request.put("instances", new JsonArray().add(angryPlanetInstance).add(treasureIslandInstance).add(dealBreakerInstance));
    request.put("totalRecords", 3);

    // Post instance
    final var postCompleted = okapiClient
      .post(ApiRoot.instancesBatch(), request);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    // Assertions
    assertThat(postResponse.getStatusCode(), is(HttpResponseStatus.CREATED.code()));
    assertEquals(postResponse.getJson().getJsonArray("instances").size(), 2);
    assertEquals(postResponse.getJson().getJsonArray("errorMessages").size(), 1);
    assertEquals(postResponse.getJson().getInteger("totalRecords"), Integer.valueOf(2));
  }

  @Test
  public void instanceTitleIsMandatory()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

    JsonObject newInstanceRequest = new JsonObject();

    final var postCompleted = okapiClient.post(
      ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(postResponse.getStatusCode(), is(400));
    assertThat(postResponse.getContentType(), is(ContentType.TEXT_PLAIN));
    assertThat(postResponse.getLocation(), is(nullValue()));
    assertThat(postResponse.getBody(), is("Title must be provided for an instance"));
  }

  @Test
  public void canUpdateAnExistingInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

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
        new Subject(null,  null, sourceId, typeId)))
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", true);

    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(),
      newInstance.getString("id")));

    Response putResponse = updateInstance(updateInstanceRequest);

    assertThat(putResponse.getStatusCode(), is(204));

    final var getCompleted = okapiClient.get(instanceLocation);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

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
  public void canUpdateAnExistingInstanceWithPrecedingSucceedingTitlesMarcSource() {
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
        .put("id", newInstance.getJsonArray("precedingTitles").getJsonObject(0).getString( "id" ))
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

    Response putResponse = updateInstance(updateInstanceRequest);

    assertThat(putResponse.getStatusCode(), is(204));
  }

  @Test
  public void canAddTagToExistingInstanceWithUnconnectedPrecedingSucceeding() {
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
        .put("id", newInstance.getJsonArray("precedingTitles").getJsonObject(0).getString( "id" ))
        .put(PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY, null)
        .put("identifiers", new JsonArray().add(
          new JsonObject()
            .put("identifierTypeId", "913300b2-03ed-469a-8179-c1092c991227")
            .put("value", "0273-656X"))
        ));
    var updateInstanceRequest = newInstance.copy()
      .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add("test")))
      .put(PRECEDING_TITLES_KEY, precedingTitles);

    var putResponse = updateInstance(updateInstanceRequest);

    assertThat(putResponse.getStatusCode(), is(204));
  }

  @Test
  public void cannotUpdateAnInstanceThatDoesNotExist() {
    JsonObject updateInstanceRequest = smallAngryPlanet(UUID.randomUUID());

    Response putResponse = updateInstance(updateInstanceRequest);

    assertThat(putResponse.getStatusCode(), is(404));
    assertThat(putResponse.getBody(), is("Instance not found"));
  }

  @Test
  public void cannotUpdateAnInstanceWithOptimisticLockingFailure() {

    JsonObject instance = createInstance(smallAngryPlanet(ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE));

    Response putResponse = updateInstance(instance);
    assertThat(putResponse.getStatusCode(), is(409));
    assertThat(putResponse.getBody(), is("Optimistic Locking"));
    assertThat(putResponse.getContentType(), is(ContentType.TEXT_PLAIN));
  }

  @Test
  public void canUpdateAnExistingMARCInstanceIfNoChanges()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID id = UUID.randomUUID();
    // Create new Instance
    JsonObject newInstance = createInstance(treasureIslandInstance(id));
    JsonObject instanceForUpdate = newInstance.copy();
    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(), newInstance.getString("id")));
    // Put Instance for update
    Response putResponse = updateInstance(instanceForUpdate);
    assertThat(putResponse.getStatusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
    // Get existing Instance
    final var getCompleted = okapiClient.get(instanceLocation);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));

    JsonObject updatedInstance = getResponse.getJson();
    assertEquals(updatedInstance, newInstance);
  }

  @Test
  public void canNotUpdateAnExistingMARCInstanceIfBlockedFieldsAreChanged() {
    UUID id = UUID.randomUUID();
    createInstance(treasureIslandInstance(id));
    JsonObject instanceForUpdate = marcInstanceWithDefaultBlockedFields(id);

    for (String field : config.getInstanceBlockedFields()) {
      // Put Instance for update
      Response putResponse = updateInstance(instanceForUpdate);

      assertThat(putResponse.getStatusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
      assertThat(putResponse.getJson().getJsonArray("errors").size(), is(1));

      instanceForUpdate.remove(field);
    }
  }

  @Test
  public void canNotUpdateAnExistingMARCInstanceIfBlockedFieldsAreChangedToNulls()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID id = UUID.randomUUID();
    JsonObject createInstanceRequest = treasureIslandInstance(id)
      .put("hrid", "test-hrid-0")
      .put("statusId", "test-statusId-0");
    // Create new Instance
    JsonObject newInstance = createInstance(createInstanceRequest);

    JsonObject instanceForUpdate = treasureIslandInstance(id);
    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(), newInstance.getString("id")));
    // Put Instance for update
    Response putResponse = updateInstance(instanceForUpdate);

    assertThat(putResponse.getStatusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
    assertNotNull(putResponse.getJson().getJsonArray("errors"));
    JsonArray errors = putResponse.getJson().getJsonArray("errors");
    assertThat(errors.size(), is(1));
    assertThat(errors.getJsonObject(0).getString("message"), is(
      "Instance is controlled by MARC record, these fields are blocked and can not be updated: " +
        "physicalDescriptions,notes,languages,precedingTitles,identifiers,instanceTypeId,modeOfIssuanceId,subjects,dates," +
        "source,title,indexTitle,publicationFrequency,electronicAccess,publicationRange," +
        "classifications,succeedingTitles,editions,hrid,series,instanceFormatIds,publication,contributors," +
        "alternativeTitles"));

    // Get existing Instance
    final var getCompleted = okapiClient.get(instanceLocation);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();
    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is(newInstance.getString("title")));
    assertThat(updatedInstance.getString("source"), is(newInstance.getString("source")));
    assertThat(updatedInstance.getString("hrid"), is(newInstance.getString("hrid")));
    assertThat(updatedInstance.getString("statusId"), is(newInstance.getString("statusId")));
  }

  @Test
  public void canUpdateAnExistingMARCInstanceIfBlockedFieldsAreNotChanged()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID id = UUID.randomUUID();
    JsonObject createInstanceRequest = treasureIslandInstance(id)
      .put("sourceRecordFormat", "test-format-0"); // 'sourceRecordFormat' is non blocked field
    // Create new Instance
    JsonObject newInstance = createInstance(createInstanceRequest);

    JsonObject instanceForUpdate = newInstance.copy()
      .put("sourceRecordFormat", "test-format-1");
    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(), newInstance.getString("id")));
    // Put Instance for update
    Response putResponse = updateInstance(instanceForUpdate);

    assertThat(putResponse.getStatusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

    // Get existing Instance
    final var getCompleted = okapiClient.get(instanceLocation);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(HttpResponseStatus.OK.code()));

    JsonObject updatedInstance = getResponse.getJson();
    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is(newInstance.getString("title")));
    assertThat(updatedInstance.getString("source"), is(newInstance.getString("source")));
    assertThat(updatedInstance.getString("sourceRecordFormat"), is(instanceForUpdate.getString("sourceRecordFormat")));
  }

  @Test
  public void canNotUpdateInstanceMarkedForDeletionIfSuppressionFlagsAreChangedToFalse() {
    JsonObject newInstanceRequest = smallAngryPlanet(UUID.randomUUID())
      .put("staffSuppress", true)
      .put("discoverySuppress", true)
      .put("deleted", true);
    JsonObject newInstance = createInstance(newInstanceRequest);
    assertTrue(newInstance.getBoolean("deleted"));

    JsonObject updateInstanceRequest = newInstance.copy()
      .put("discoverySuppress", false)
      .put("staffSuppress", false);

    Response putResponse = updateInstance(updateInstanceRequest);

    assertThat(putResponse.getStatusCode(), is(400));
    assertTrue(putResponse.hasBody());
    assertEquals(FLAGS_SUPPRESSION_INCONSISTENCY_MESSAGE, putResponse.getBody());
  }

  @Test
  public void canDeleteAllInstances()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));

    final var deleteCompleted = okapiClient.delete(
        ApiRoot.instances() + "?query=" + PercentCodec.encode("cql.allRecords=1"));

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    final var getAllCompleted = okapiClient.get(ApiRoot.instances());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  public void canDeleteAnInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));

    JsonObject instanceToDelete = createInstance(leviathanWakes(UUID.randomUUID()));

    URL instanceToDeleteLocation = new URL(String.format("%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id")));

    final var deleteCompleted = okapiClient.delete(instanceToDeleteLocation);

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    final var getCompleted = okapiClient.get(instanceToDeleteLocation);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));

    final var getAllCompleted = okapiClient.get(ApiRoot.instances());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  @SneakyThrows
  public void canSoftDeleteInstance() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    JsonObject sourceRecord = new JsonObject().put("id", instanceId.toString());

    sourceRecordStorageClient.create(sourceRecord);
    Response getCreatedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getCreatedSourceRecordResponse.getStatusCode(), HttpStatus.HTTP_OK.toInt());
    assertEquals(instanceId.toString(), getCreatedSourceRecordResponse.getJson().getString("id"));

    URL softDeleteUrl = new URL(String.format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted" ));

    URL getByIdUrl = new URL(String.format("%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id")));

    final var deleteCompleted = okapiClient.delete(softDeleteUrl);

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    final var getCompleted = okapiClient.get(getByIdUrl);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));
    assertTrue(getResponse.getJson().getBoolean("deleted"));

    Response getDeletedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getDeletedSourceRecordResponse.getStatusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());
  }

  @Test
  @SneakyThrows
  public void canSoftDeleteInstanceIfSourceRecordNotFound() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    Response getSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getSourceRecordResponse.getStatusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());

    URL softDeleteUrl = new URL(String.format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted" ));

    URL getByIdUrl = new URL(String.format("%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id")));

    final var deleteCompleted = okapiClient.delete(softDeleteUrl);

    Response deleteResponse = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    String expectedMessage = String.format(
      "MARC record was not set for deletion because it was not found by instance ID: %s", instanceId);
    assertThat(deleteResponse.getStatusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(deleteResponse.hasBody(), is(true));
    assertThat(deleteResponse.getBody(), is(expectedMessage));

    final var getCompleted = okapiClient.get(getByIdUrl);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));

    Response getDeletedSourceRecordResponse = sourceRecordStorageClient.getById(instanceId);
    assertEquals(getDeletedSourceRecordResponse.getStatusCode(), HttpStatus.HTTP_NOT_FOUND.toInt());
  }

  @Test
  @SneakyThrows
  public void canSoftDeleteInstanceIfFailedToMarkSourceRecordAsDeleted() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instanceToDelete = createInstance(marcInstanceWithDefaultBlockedFields(instanceId));

    sourceRecordStorageClient.emulateFailure(new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now(UTC).plusSeconds(2).toDate())
      .setBody("Internal server error")
      .setContentType("plain/text")
      .setStatusCode(500)
      .setMethod(DELETE.name()));

    URL softDeleteUrl = URI.create(String.format("%s/%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id"), "mark-deleted" )).toURL();
    URL getByIdUrl = URI.create(
      String.format("%s/%s", ApiRoot.instances(), instanceToDelete.getString("id"))).toURL();

    final var deleteCompleted = okapiClient.delete(softDeleteUrl);
    Response deleteResponse = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(HTTP_INTERNAL_SERVER_ERROR.toInt()));
    assertThat(deleteResponse.hasBody(), is(true));

    final var getCompleted = okapiClient.get(getByIdUrl);

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);
    assertTrue(getResponse.getJson().getBoolean("staffSuppress"));
    assertTrue(getResponse.getJson().getBoolean("discoverySuppress"));
  }

  @Test
  public void canGetAllInstances()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));

    final var getAllCompleted = okapiClient.get(ApiRoot.instances());

    Response getAllResponse = getAllCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getAllResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(3));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(3));
  }

  @Test
  public void canPageAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));
    createInstance(taoOfPooh(UUID.randomUUID()));

    final var firstPageGetCompleted
      = okapiClient.get(ApiRoot.instances("limit=3"));

    final var secondPageGetCompleted
      = okapiClient.get(ApiRoot.instances("limit=3&offset=3"));

    Response firstPageResponse = firstPageGetCompleted.toCompletableFuture().get(5, SECONDS);
    Response secondPageResponse = secondPageGetCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(firstPageResponse.getStatusCode(), is(200));
    assertThat(secondPageResponse.getStatusCode(), is(200));

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
  public void pageParametersMustBeNumeric()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    final var getPagedCompleted = okapiClient.get(ApiRoot.instances("limit=&offset="));

    Response getPagedResponse = getPagedCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getPagedResponse.getStatusCode(), is(400));
    assertThat(getPagedResponse.getBody(),
      is("limit and offset must be numeric when supplied"));
  }

  @Test
  public void canSearchForInstancesByTitle()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(uprooted(UUID.randomUUID()));

    final var searchGetCompleted
      = okapiClient.get(ApiRoot.instances("query=title=Small%20Angry*"));

    Response searchGetResponse = searchGetCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(searchGetResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(1));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(1));
    assertThat(instances.get(0).getString("title"), is("Long Way to a Small Angry Planet"));
  }

  @Test
  public void cannotFindAnUnknownInstance()
    throws InterruptedException,
    TimeoutException,
    ExecutionException {

    final var getCompleted
      = okapiClient.get(String.format("%s/%s", ApiRoot.instances(), UUID.randomUUID()));

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));
  }

  @Test
  public void cannotChangeHRID() {
    UUID instanceId = UUID.randomUUID();
    JsonObject createdInstance = createInstance(smallAngryPlanet(instanceId));

    assertThat(createdInstance.getString("hrid"), notNullValue());

    JsonObject instanceToUpdate = createdInstance.copy()
      .put("title", "updatedTitle")
      .put("hrid", "updatedHrid");

    Response instanceUpdateResponse = updateInstance(instanceToUpdate);

    assertThat(instanceUpdateResponse,
      hasValidationError("HRID can not be updated", "hrid", "updatedHrid"));

    JsonObject existingInstance = instancesClient.getById(instanceId).getJson();
    assertThat(existingInstance, is(createdInstance));
  }

  @Test
  public void cannotRemoveHRID() {
    UUID instanceId = UUID.randomUUID();
    JsonObject createdInstance = createInstance(smallAngryPlanet(instanceId));

    assertThat(createdInstance.getString("hrid"), notNullValue());

    JsonObject instanceToUpdate = createdInstance.copy()
      .put("title", "updatedTitle");

    instanceToUpdate.remove("hrid");

    Response instanceUpdateResponse = updateInstance(instanceToUpdate);

    assertThat(instanceUpdateResponse,
      hasValidationError("HRID can not be updated", "hrid", null));

    JsonObject existingInstance = instancesClient.getById(instanceId).getJson();
    assertThat(existingInstance, is(createdInstance));
  }

  @Test
  public void canFrowardInstanceCreateFailureFromStorage() throws Exception {
    final String expectedErrorMessage = "Instance-storage is temporary unavailable for create";

    instancesStorageClient.emulateFailure(new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now(UTC).plusSeconds(2).toDate())
      .setBody(expectedErrorMessage)
      .setContentType("plain/text")
      .setStatusCode(500)
      .setMethod(POST.name()));

    final Response response = instancesClient.attemptToCreate(smallAngryPlanet(UUID.randomUUID()));

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getBody(), is(expectedErrorMessage));
  }

  @Test
  public void canFrowardInstanceUpdateFailureFromStorage() throws Exception {
    final String expectedErrorMessage = "Instance-storage is temporary unavailable for updates";

    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    instancesStorageClient.emulateFailure(new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now(UTC).plusSeconds(2).toDate())
      .setBody(expectedErrorMessage)
      .setContentType("plain/text")
      .setStatusCode(500)
      .setMethod(PUT.name()));

    final Response updateResponse = instancesClient
      .attemptToReplace(instance.getId(), instance.getJson().copy()
        .put("subjects", new JsonArray().add("Small angry planet subject")));

    assertThat(updateResponse.getStatusCode(), is(500));
    assertThat(updateResponse.getBody(), is(expectedErrorMessage));
  }

  @Test
  public void canFrowardInstanceCreateValidationErrorFromStorage() throws Exception {
    final String expectedErrorMessage = "A note has exceeded the 32000 character limit.";

    instancesStorageClient.emulateFailure(new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now(UTC).plusSeconds(2).toDate())
        .setBody(expectedErrorMessage)
        .setContentType("plain/text")
        .setStatusCode(422)
        .setMethod(POST.name()));

    final Response response = instancesClient.attemptToCreate(smallAngryPlanet(UUID.randomUUID()));

    assertThat(response.getStatusCode(), is(422));
    assertThat(response.getBody(), is(expectedErrorMessage));
  }

  @Test
  public void canFrowardInstanceUpdateValidationErrorFromStorage() throws Exception {
    final String expectedErrorMessage = "A note has exceeded the 32000 character limit.";

    final IndividualResource instance = instancesClient
        .create(smallAngryPlanet(UUID.randomUUID()));

    instancesStorageClient.emulateFailure(new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now(UTC).plusSeconds(2).toDate())
        .setBody(expectedErrorMessage)
        .setContentType("plain/text")
        .setStatusCode(422)
        .setMethod(PUT.name()));

    final Response updateResponse = instancesClient
        .attemptToReplace(instance.getId(), instance.getJson().copy()
            .put("subjects", new JsonArray().add("Small angry planet subject")));

    assertThat(updateResponse.getStatusCode(), is(422));
    assertThat(updateResponse.getBody(), is(expectedErrorMessage));
  }

  @SneakyThrows
  private JsonObject createInstance(JsonObject newInstanceRequest) {
    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  private static void containsApiRoot(String link) {
    assertThat(link.contains(ApiTestSuite.apiRoot()), is(true));
  }

  @SneakyThrows
  private Response updateInstance(JsonObject instance) {
    String instanceUpdateUri = String
      .format("%s/%s", ApiRoot.instances(), instance.getString("id"));

    final var putFuture = okapiClient.put(instanceUpdateUri, instance);

    return putFuture.toCompletableFuture().get(5, SECONDS);
  }
}
