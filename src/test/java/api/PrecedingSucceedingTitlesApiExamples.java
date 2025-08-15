package api;

import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static api.support.InstanceSamples.taoOfPooh;
import static api.support.InstanceSamples.uprooted;
import static org.folio.inventory.domain.instances.Instance.PRECEDING_TITLES_KEY;
import static org.folio.inventory.domain.instances.Instance.SUCCEEDING_TITLES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class PrecedingSucceedingTitlesApiExamples extends ApiTests {

  @Test
  public void canCreateAnInstanceWithUnconnectedPrecedingTitles() {
    String precedingSucceedingTitleId1 = UUID.randomUUID().toString();
    String precedingSucceedingTitleId2 = UUID.randomUUID().toString();
    JsonArray precedingTitles = getUnconnectedPrecedingSucceedingTitle(
      precedingSucceedingTitleId1, precedingSucceedingTitleId2);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", precedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonArray actualPrecedingTitles = createdInstance.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, precedingSucceedingTitleId1);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, precedingSucceedingTitleId2);

    assertPrecedingTitles(actualPrecedingTitle1, precedingTitles.getJsonObject(0), null);
    assertPrecedingTitles(actualPrecedingTitle2, precedingTitles.getJsonObject(1), null);
  }

  @Test
  public void canCreateAnInstanceWithUnconnectedSucceedingTitles() {
    String precedingSucceedingTitleId1 = UUID.randomUUID().toString();
    String precedingSucceedingTitleId2 = UUID.randomUUID().toString();
    JsonArray succeedingTitles = getUnconnectedPrecedingSucceedingTitle(
      precedingSucceedingTitleId1, precedingSucceedingTitleId2);
    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", succeedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonArray actualSucceedingTitles = createdInstance.getJson().getJsonArray("succeedingTitles");
    JsonObject actualSucceedingTitle1 = getRecordById(actualSucceedingTitles, precedingSucceedingTitleId1);
    JsonObject actualSucceedingTitle2 = getRecordById(actualSucceedingTitles, precedingSucceedingTitleId2);

    assertSucceedingTitles(actualSucceedingTitle1, succeedingTitles.getJsonObject(0), null);
    assertSucceedingTitles(actualSucceedingTitle2, succeedingTitles.getJsonObject(1), null);
  }

  @Test
  public void canUpdateAnInstanceWithUnconnectedPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    JsonObject precedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", new JsonArray().add(precedingTitle));

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonObject newPrecedingTitle = new JsonObject()
      .put("title", "New title")
      .put("hrid", "inst000000000101")
      .put("identifiers", new JsonArray().add(
        new JsonObject()
          .put("identifierTypeId", "1231054f-be78-422d-bd51-4ed9f33c3422")
          .put("value", "000012104")));

    JsonObject newInstance = createdInstance.getJson().copy();
    newInstance.put("precedingTitles", new JsonArray().add(newPrecedingTitle));

    instancesClient.replace(createdInstance.getId(), newInstance);

    Response instanceResponse = instancesClient.getById(createdInstance.getId());
    assertPrecedingTitles(instanceResponse.getJson().getJsonArray("precedingTitles")
      .getJsonObject(0), newPrecedingTitle, null);
  }

  @Test
  public void canUpdateAnInstanceWithUnconnectedSucceedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    JsonObject succeedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", new JsonArray().add(succeedingTitle));

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonObject newSucceedingTitle = new JsonObject()
      .put("title", "New title")
      .put("hrid", "inst000000000022")
      .put("identifiers", new JsonArray().add(
        new JsonObject()
          .put("identifierTypeId", "8261054f-be78-422d-bd51-4ed9f33c3422")
          .put("value", "0262012103")));

    JsonObject newInstance = createdInstance.getJson().copy();
    newInstance.put("succeedingTitles", new JsonArray().add(newSucceedingTitle));

    instancesClient.replace(createdInstance.getId(), newInstance);

    Response instanceResponse = instancesClient.getById(createdInstance.getId());
    assertSucceedingTitles(instanceResponse.getJson().getJsonArray("succeedingTitles")
      .getJsonObject(0), newSucceedingTitle, null);
  }

  @Test
  public void titleIsRequiredToCreateAnInstanceWithUnconnectedSucceedingTitle()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final JsonObject succeedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    succeedingTitle.remove("title");

    final JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID())
      .put(SUCCEEDING_TITLES_KEY, new JsonArray()
        .add(createSemanticWebUnconnectedTitle(UUID.randomUUID().toString()))
        .add(succeedingTitle));

    final Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Title is required for unconnected succeeding title",
      SUCCEEDING_TITLES_KEY + ".title", null));
  }

  @Test
  public void titleIsRequiredToUpdateAnInstanceWithUnconnectedSucceedingTitle()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final JsonObject succeedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    final JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID())
      .put(SUCCEEDING_TITLES_KEY, new JsonArray()
        .add(succeedingTitle));

    final IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    final JsonObject newSucceedingTitle = succeedingTitle.copy();
    newSucceedingTitle.remove("title");

    final Response response = instancesClient.attemptToReplace(createdInstance.getId(),
      createdInstance.getJson().copy().put(SUCCEEDING_TITLES_KEY, new JsonArray()
        .add(succeedingTitle)
        .add(newSucceedingTitle)));

    assertThat(response, hasValidationError("Title is required for unconnected succeeding title",
      SUCCEEDING_TITLES_KEY + ".title", null));
  }

  @Test
  public void titleIsRequiredToCreateAnInstanceWithUnconnectedPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final JsonObject precedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    precedingTitle.remove("title");

    final JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID())
      .put(PRECEDING_TITLES_KEY, new JsonArray()
        .add(createSemanticWebUnconnectedTitle(UUID.randomUUID().toString()))
        .add(precedingTitle));

    final Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Title is required for unconnected preceding title",
      PRECEDING_TITLES_KEY + ".title", null));
  }

  @Test
  public void titleIsRequiredToUpdateAnInstanceWithUnconnectedPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final JsonObject precedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    final JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID())
      .put(PRECEDING_TITLES_KEY, new JsonArray().add(precedingTitle));

    final IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    final JsonObject newPrecedingTitle = precedingTitle.copy();
    newPrecedingTitle.remove("title");

    final Response response = instancesClient.attemptToReplace(createdInstance.getId(),
      createdInstance.getJson().copy().put(PRECEDING_TITLES_KEY, new JsonArray()
        .add(precedingTitle)
        .add(newPrecedingTitle)));

    assertThat(response, hasValidationError("Title is required for unconnected preceding title",
      PRECEDING_TITLES_KEY + ".title", null));
  }

  @Test
  public void titleIsRequiredToCreateInstancesUsingBatch() {

    final JsonObject instanceWithValidPrecedingTitle = smallAngryPlanet(UUID.randomUUID())
      .put(PRECEDING_TITLES_KEY, new JsonArray()
        .add(createOpenBibliographyUnconnectedTitle(UUID.randomUUID().toString())));

    final JsonObject unconnectedTitleNoTitle = createOpenBibliographyUnconnectedTitle(
      UUID.randomUUID().toString());
    unconnectedTitleNoTitle.remove("title");

    final JsonObject instanceWithInvalidPrecedingTitle = smallAngryPlanet(UUID.randomUUID())
      .put(PRECEDING_TITLES_KEY, new JsonArray().add(unconnectedTitleNoTitle));

    final JsonObject instanceWithValidSucceedingTitle = smallAngryPlanet(UUID.randomUUID())
      .put(SUCCEEDING_TITLES_KEY, new JsonArray()
        .add(createSemanticWebUnconnectedTitle(UUID.randomUUID().toString())));

    final JsonObject semanticWebUnconnectedTitleNoTitle = createSemanticWebUnconnectedTitle(
      UUID.randomUUID().toString());
    semanticWebUnconnectedTitleNoTitle.remove("title");

    final JsonObject instanceWithInvalidSucceedingTitle = smallAngryPlanet(UUID.randomUUID())
      .put(SUCCEEDING_TITLES_KEY, new JsonArray().add(semanticWebUnconnectedTitleNoTitle));


    final JsonObject request = new JsonObject()
      .put("instances", new JsonArray().add(instanceWithInvalidPrecedingTitle)
        .add(instanceWithValidPrecedingTitle)
        .add(instanceWithValidSucceedingTitle)
        .add(instanceWithInvalidSucceedingTitle))
      .put("totalRecords", 4);

    final JsonObject response = instancesBatchClient.create(request)
      .getJson();

    assertThat(response.getJsonArray("errorMessages").size(), is(2));

    final Map<String, JsonObject> createdValidInstances = JsonArrayHelper
      .toList(response.getJsonArray("instances")).stream()
      .collect(Collectors.toMap(json -> json.getString("id"), Function.identity()));

    Arrays.asList(instanceWithValidPrecedingTitle, instanceWithValidSucceedingTitle)
      .forEach(validInstance -> assertThat(createdValidInstances
        .get(validInstance.getString("id")), notNullValue()));
  }

  @Test
  public void canDeleteUnconnectedPrecedingTitle()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    JsonObject precedingTitle = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", new JsonArray().add(precedingTitle));

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonObject newInstance = createdInstance.getJson().copy();
    newInstance.put("precedingTitles", new JsonArray());

    instancesClient.replace(createdInstance.getId(), newInstance);

    Response instanceResponse = instancesClient.getById(createdInstance.getId());
    assertThat(instanceResponse.getJson().getJsonArray("precedingTitles"), is(new JsonArray()));
  }

  @Test
  public void canDeleteUnconnectedSucceedingTitle()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    JsonObject succeedingTitles = createSemanticWebUnconnectedTitle(UUID.randomUUID().toString());
    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", new JsonArray().add(succeedingTitles));

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    JsonObject newInstance = createdInstance.getJson().copy();
    newInstance.put("succeedingTitles", new JsonArray());

    instancesClient.replace(createdInstance.getId(), newInstance);

    Response instanceResponse = instancesClient.getById(createdInstance.getId());
    assertThat(instanceResponse.getJson().getJsonArray("succeedingTitles"), is(new JsonArray()));
  }

  @Test
  public void canCreateAnInstanceWithConnectedPrecedingTitles() {
    UUID nodId = UUID.randomUUID();
    UUID uprootedId = UUID.randomUUID();
    String nodPrecedingTitleId = UUID.randomUUID().toString();
    String uprootedPrecedingTitleId = UUID.randomUUID().toString();
    IndividualResource nod = instancesClient.create(nod(nodId));
    IndividualResource uprooted = instancesClient.create(uprooted(uprootedId));

    JsonObject nodPrecedingTitle = createConnectedPrecedingTitle(nodPrecedingTitleId, nodId.toString());
    JsonObject uprootedPrecedingTitle = createConnectedPrecedingTitle(uprootedPrecedingTitleId, uprootedId.toString());

    JsonArray precedingTitles = new JsonArray()
      .add(nodPrecedingTitle).add(uprootedPrecedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", precedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    // Fetch the created instance because POST response body does not include all data for the preceding/succeeding titles fields
    Response getResponse = instancesClient.getById(createdInstance.getId());
    assertThat(getResponse.getStatusCode(), is(200));
    JsonArray actualPrecedingTitles = getResponse.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, nodPrecedingTitleId);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, uprootedPrecedingTitleId);

    assertPrecedingTitles(actualPrecedingTitle1, nod.getJson(), nod.getId().toString());
    assertPrecedingTitles(actualPrecedingTitle2, uprooted.getJson(), uprooted.getId().toString());

    verifyRelatedInstanceSucceedingTitle(nod, createdInstance);
    verifyRelatedInstanceSucceedingTitle(uprooted, createdInstance);
  }

  @Test
  public void canCreateAnInstanceWithConnectedSucceedingTitles() {
    UUID nodId = UUID.randomUUID();
    UUID uprootedId = UUID.randomUUID();
    String nodPrecedingTitleId = UUID.randomUUID().toString();
    String uprootedPrecedingTitleId = UUID.randomUUID().toString();
    IndividualResource nod = instancesClient.create(nod(nodId));
    IndividualResource uprooted = instancesClient.create(uprooted(uprootedId));

    JsonObject nodSucceedingTitle = createConnectedSucceedingTitle(nodPrecedingTitleId, nodId.toString());
    JsonObject uprootedSucceedingTitle = createConnectedSucceedingTitle(uprootedPrecedingTitleId, uprootedId.toString());

    JsonArray succeedingTitles = new JsonArray()
      .add(nodSucceedingTitle).add(uprootedSucceedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", succeedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    Response getResponse = instancesClient.getById(createdInstance.getId());
    assertThat(getResponse.getStatusCode(), is(200));
    JsonArray actualSucceedingTitles = getResponse.getJson().getJsonArray("succeedingTitles");
    JsonObject actualSucceedingTitle1 = getRecordById(actualSucceedingTitles, nodPrecedingTitleId);
    JsonObject actualSucceedingTitle2 = getRecordById(actualSucceedingTitles, uprootedPrecedingTitleId);

    assertSucceedingTitles(actualSucceedingTitle1, nod.getJson(), nod.getId().toString());
    assertSucceedingTitles(actualSucceedingTitle2, uprooted.getJson(), uprooted.getId().toString());

    verifyRelatedInstancePrecedingTitle(nod, createdInstance);
    verifyRelatedInstancePrecedingTitle(uprooted, createdInstance);
  }

  @Test
  public void canUpdateAnInstanceWithConnectedPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    UUID nodId = UUID.randomUUID();
    UUID uprootedId = UUID.randomUUID();
    String nodPrecedingTitleId = UUID.randomUUID().toString();
    String uprootedPrecedingTitleId = UUID.randomUUID().toString();
    IndividualResource nod = instancesClient.create(nod(nodId));
    IndividualResource uprooted = instancesClient.create(uprooted(uprootedId));

    JsonObject nodPrecedingTitle = createConnectedPrecedingTitle(nodPrecedingTitleId, nodId.toString());
    JsonObject uprootedPrecedingTitle = createConnectedPrecedingTitle(uprootedPrecedingTitleId, uprootedId.toString());

    JsonArray precedingTitles = new JsonArray()
      .add(nodPrecedingTitle).add(uprootedPrecedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", precedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    IndividualResource taoOfPooh = instancesClient.create(taoOfPooh(uprootedId));
    JsonObject newInstance = createdInstance.getJson().copy();
    JsonObject taoOfPoohPrecedingTitle = createConnectedPrecedingTitle(UUID.randomUUID().toString(), taoOfPooh
      .getId().toString());
    newInstance.put("precedingTitles", new JsonArray()
      .add(taoOfPoohPrecedingTitle));

    instancesClient.replace(createdInstance.getId(), newInstance);

    verifyRelatedInstancePrecedingTitle(createdInstance, taoOfPooh);
    verifyRelatedInstanceSucceedingTitle(taoOfPooh, createdInstance);
  }

  @Test
  public void canUpdateAnInstanceWithConnectedSucceedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    UUID nodId = UUID.randomUUID();
    UUID uprootedId = UUID.randomUUID();
    String nodPrecedingTitleId = UUID.randomUUID().toString();
    String uprootedPrecedingTitleId = UUID.randomUUID().toString();
    IndividualResource nod = instancesClient.create(nod(nodId));
    IndividualResource uprooted = instancesClient.create(uprooted(uprootedId));

    JsonObject nodSucceedingTitle = createConnectedSucceedingTitle(nodPrecedingTitleId, nodId.toString());
    JsonObject uprootedSucceedingTitle = createConnectedSucceedingTitle(uprootedPrecedingTitleId, uprootedId.toString());

    JsonArray succeedingTitles = new JsonArray()
      .add(nodSucceedingTitle).add(uprootedSucceedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", succeedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    IndividualResource taoOfPooh = instancesClient.create(taoOfPooh(uprootedId));
    JsonObject newInstance = createdInstance.getJson().copy();
    JsonObject taoOfPoohPrecedingTitle = createConnectedSucceedingTitle(UUID.randomUUID().toString(), taoOfPooh
      .getId().toString());
    newInstance.put("succeedingTitles", new JsonArray()
      .add(taoOfPoohPrecedingTitle));

    instancesClient.replace(createdInstance.getId(), newInstance);

    verifyRelatedInstanceSucceedingTitle(createdInstance, taoOfPooh);
    verifyRelatedInstancePrecedingTitle(taoOfPooh, createdInstance);
  }

  @Test
  public void canUpdateAnInstanceWithConnectedSucceedingTitleIfPrecedingAlreadySet()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final IndividualResource nod = instancesClient.create(nod(UUID.randomUUID()));
    final IndividualResource uprooted = instancesClient.create(uprooted(UUID.randomUUID()));
    final String nodPrecedingTitleId = UUID.randomUUID().toString();
    final String uprootedPrecedingTitleId = UUID.randomUUID().toString();

    JsonObject nodSucceedingTitle = createConnectedSucceedingTitle(nodPrecedingTitleId,
      nod.getId().toString());
    JsonObject uprootedPrecedingTitle = createConnectedPrecedingTitle(uprootedPrecedingTitleId,
      uprooted.getId().toString());

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanet(UUID.randomUUID())
      .put("precedingTitles", new JsonArray().add(uprootedPrecedingTitle)));

    instancesClient.replace(createdInstance.getId(), createdInstance.copyJson()
      .put("succeedingTitles", new JsonArray()
        .add(nodSucceedingTitle)));

    verifyRelatedInstancePrecedingTitle(createdInstance, uprooted);
    verifyRelatedInstanceSucceedingTitle(createdInstance, nod);
  }

  @Test
  public void canCreateAnInstanceWithConnectedSucceedingAndPrecedingTitles() {
    UUID nodId = UUID.randomUUID();
    UUID uprootedId = UUID.randomUUID();
    String nodPrecedingTitleId = UUID.randomUUID().toString();
    String uprootedSucceedingTitleId = UUID.randomUUID().toString();
    IndividualResource nod = instancesClient.create(nod(nodId));
    IndividualResource uprooted = instancesClient.create(uprooted(uprootedId));

    JsonObject nodPrecedingTitle = createConnectedPrecedingTitle(nodPrecedingTitleId, nodId.toString());
    JsonObject uprootedSucceedingTitle = createConnectedSucceedingTitle(uprootedSucceedingTitleId, uprootedId.toString());

    String unconnectedSucceedingTitleId = UUID.randomUUID().toString();
    String unconnectedPrecedingTitleId = UUID.randomUUID().toString();

    JsonObject unconnectedSucceedingTitle = createSemanticWebUnconnectedTitle(unconnectedSucceedingTitleId);
    JsonObject unconnectedPrecedingTitle = createOpenBibliographyUnconnectedTitle(unconnectedPrecedingTitleId);

    JsonArray precedingTitles = new JsonArray()
      .add(nodPrecedingTitle).add(unconnectedPrecedingTitle);
    JsonArray succeedingTitles = new JsonArray()
      .add(uprootedSucceedingTitle).add(unconnectedSucceedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", precedingTitles);
    smallAngryPlanetJson.put("succeedingTitles", succeedingTitles);

    IndividualResource createdInstance = instancesClient.create(smallAngryPlanetJson);

    Response getResponse = instancesClient.getById(createdInstance.getId());
    assertThat(getResponse.getStatusCode(), is(200));
    JsonArray actualPrecedingTitles = getResponse.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, nodPrecedingTitleId);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, unconnectedPrecedingTitleId);

    assertPrecedingTitles(actualPrecedingTitle1, nod.getJson(), nod.getId().toString());
    assertPrecedingTitles(actualPrecedingTitle2, unconnectedPrecedingTitle, null);

    JsonArray actualSucceedingTitles = getResponse.getJson().getJsonArray("succeedingTitles");
    JsonObject actualSucceedingTitle1 = getRecordById(actualSucceedingTitles, uprootedSucceedingTitleId);
    JsonObject actualSucceedingTitle2 = getRecordById(actualSucceedingTitles, unconnectedSucceedingTitleId);

    assertSucceedingTitles(actualSucceedingTitle1, uprooted.getJson(), uprooted.getId().toString());
    assertSucceedingTitles(actualSucceedingTitle2, unconnectedSucceedingTitle, null);

    verifyRelatedInstancePrecedingTitle(uprooted, createdInstance);
    verifyRelatedInstanceSucceedingTitle(nod, createdInstance);
  }

  @Test
  public void canCreateBatchOfInstancesWithPrecedingSucceedingTitles() {
    String precedingSucceedingTitleId1 = UUID.randomUUID().toString();
    String precedingSucceedingTitleId2 = UUID.randomUUID().toString();
    JsonArray precedingTitles = getUnconnectedPrecedingSucceedingTitle(
      precedingSucceedingTitleId1, precedingSucceedingTitleId2);

    UUID angryPlanetId = UUID.randomUUID();
    JsonObject angryPlanet = smallAngryPlanet(angryPlanetId)
      .put("precedingTitles", precedingTitles);

    String succeedingConnectedTitleId = UUID.randomUUID().toString();
    JsonObject succeedingConnectedTitle = createConnectedSucceedingTitle(
      succeedingConnectedTitleId, angryPlanetId.toString());
    UUID uprootedId = UUID.randomUUID();
    JsonObject uprooted = uprooted(uprootedId)
      .put("succeedingTitles", new JsonArray().add(succeedingConnectedTitle));
    JsonObject request = new JsonObject();
    request.put("instances", new JsonArray().add(angryPlanet).add(uprooted));
    request.put("totalRecords", 2);

    instancesBatchClient.create(request);

    Response createdAngryPlanet = instancesClient.getById(angryPlanetId);
    Response createdUprooted = instancesClient.getById(uprootedId);

    JsonArray actualPrecedingTitles = createdAngryPlanet.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, precedingSucceedingTitleId1);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, precedingSucceedingTitleId2);
    JsonObject actualPrecedingTitle3 = getRecordById(actualPrecedingTitles, succeedingConnectedTitleId);

    assertPrecedingTitles(actualPrecedingTitle1,  precedingTitles.getJsonObject(0), null);
    assertPrecedingTitles(actualPrecedingTitle2, precedingTitles.getJsonObject(1), null);
    assertPrecedingTitles(actualPrecedingTitle3, createdUprooted.getJson(), uprootedId.toString());

    JsonArray succeedingTitles = createdUprooted.getJson().getJsonArray("succeedingTitles");
    assertSucceedingTitles(succeedingTitles.getJsonObject(0), createdAngryPlanet.getJson(), angryPlanetId.toString());
  }

  private void verifyRelatedInstancePrecedingTitle(IndividualResource precedingInstance,
    IndividualResource succeedingInstance) {

    Response response = instancesClient.getById(precedingInstance.getId());
    JsonArray precedingTitles = response.getJson().getJsonArray("precedingTitles");
    assertPrecedingTitles(precedingTitles.getJsonObject(0), succeedingInstance.getJson(),
      succeedingInstance.getId().toString());
  }

  private void verifyRelatedInstanceSucceedingTitle(IndividualResource succeedingInstance,
    IndividualResource precedingInstance) {

    Response response = instancesClient.getById(succeedingInstance.getId());
    JsonArray succeedingTitles = response.getJson().getJsonArray("succeedingTitles");
    assertSucceedingTitles(succeedingTitles.getJsonObject(0), precedingInstance.getJson(),
      precedingInstance.getId().toString());
  }

  private JsonObject createConnectedPrecedingTitle(String id, String precedingInstanceId) {
    return new JsonObject()
      .put("id", id)
      .put("precedingInstanceId", precedingInstanceId);
  }

  private JsonObject createConnectedSucceedingTitle(String id, String succeedingInstanceId) {
    return new JsonObject()
      .put("id", id)
      .put("succeedingInstanceId", succeedingInstanceId);
  }

  private JsonObject getRecordById(JsonArray collection, String id) {
    return collection.stream()
      .map(index -> (JsonObject) index)
      .filter(request -> StringUtils.equals(request.getString("id"), id))
      .findFirst()
      .orElse(null);
  }

  private JsonArray getUnconnectedPrecedingSucceedingTitle(
    String id1, String id2) {

    JsonObject precedingSucceedingTitle1 = createSemanticWebUnconnectedTitle(id1);
    JsonObject precedingSucceedingTitle2 = createOpenBibliographyUnconnectedTitle(id2);

    JsonArray precedingSucceedingTitles = new JsonArray();
    precedingSucceedingTitles.add(precedingSucceedingTitle1);
    precedingSucceedingTitles.add(precedingSucceedingTitle2);
    return precedingSucceedingTitles;
  }

  private JsonObject createOpenBibliographyUnconnectedTitle(String precedingSucceedingTitleId2) {
    return new JsonObject()
      .put("id", precedingSucceedingTitleId2)
      .put("title", "Open Bibliography for Science, Technology and Medicine")
      .put("hrid", "inst000000000555")
      .put("identifiers", new JsonArray().add(
        new JsonObject()
          .put("identifierTypeId", "8261054f-be78-422d-bd51-4ed9f33c8012")
          .put("value", "0662012103")));
  }

  private JsonObject createSemanticWebUnconnectedTitle(String precedingSucceedingTitleId1) {
    return new JsonObject()
      .put("id", precedingSucceedingTitleId1)
      .put("title", "A semantic web prime")
      .put("hrid", "inst000000000022")
      .put("identifiers", new JsonArray().add(
        new JsonObject()
          .put("identifierTypeId", "8261054f-be78-422d-bd51-4ed9f33c3422")
          .put("value", "0262012103")));
  }

  private void assertPrecedingTitles(JsonObject actualPrecedingTitle,
    JsonObject expected, String precedingInstanceId) {

    assertThat(actualPrecedingTitle.getString("title"), is(expected.getString("title")));
    assertThat(actualPrecedingTitle.getString("hrid"), is(expected.getString("hrid")));
    assertThat(actualPrecedingTitle.getJsonArray("identifiers"), is(expected.getJsonArray("identifiers")));
    assertThat(actualPrecedingTitle.getString("precedingInstanceId"), is(precedingInstanceId));
    assertNull(actualPrecedingTitle.getString("succeedingInstanceId"));
  }

  private void assertSucceedingTitles(JsonObject actualSucceedingTitle,
    JsonObject expected, String succeedingInstanceId) {

    assertThat(actualSucceedingTitle.getString("title"), is(expected.getString("title")));
    assertThat(actualSucceedingTitle.getString("hrid"), is(expected.getString("hrid")));
    assertThat(actualSucceedingTitle.getJsonArray("identifiers"), is(expected.getJsonArray("identifiers")));
    assertThat(actualSucceedingTitle.getString("succeedingInstanceId"), is(succeedingInstanceId));
    assertNull(actualSucceedingTitle.getString("precedingInstanceId"));
  }
}
