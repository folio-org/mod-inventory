package api;

import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import support.fakes.EndpointFailureDescriptor;

public class InstanceRelationshipsTest extends ApiTests {
  private static final String PARENT_INSTANCES = "parentInstances";
  private static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  private static final String CONSORTIUM_ID_FIELD = "consortiumId";

  @After
  public void disableFailureEmulationAndClearConsortia() throws Exception {
    precedingSucceedingTitlesClient.disableFailureEmulation();
    instanceRelationshipClient.disableFailureEmulation();
    userTenantsClient.deleteAll();
    consortiaClient.deleteAll();
  }

  @Test
  public void canFetchMultipleInstancesWithRelationships() throws Exception {
    final int expectedCount = 200;
    final Map<String, String> superInstanceSubInstanceMap =
      createSuperInstanceSubInstance(expectedCount / 2);

    final Map<String, JsonObject> foundInstances = instancesClient
      .getMany("title=(\"super\" or \"sub\"", expectedCount).stream()
      .collect(Collectors.toMap(json -> json.getString("id"), Function.identity()));

    assertThat(foundInstances.size(), is(expectedCount));

    superInstanceSubInstanceMap.forEach((superInstanceId, subInstanceId) -> {
      final JsonObject superInstance = foundInstances.get(superInstanceId);
      final JsonObject subInstance = foundInstances.get(subInstanceId);

      verifyInstancesInRelationship(superInstance, subInstance);
    });
  }

  @Test
  public void canFetchOneInstancesWith11Children() throws Exception {
    UUID superInstanceId = UUID.randomUUID();
    instancesClient.create(smallAngryPlanet(superInstanceId)
        .put("title", randomString("super")));

    for (int i = 0; i < 11; i++) {
      UUID subInstanceId = UUID.randomUUID();
      instancesClient.create(nod(subInstanceId).put("title", randomString("sub")));
      instanceRelationshipClient.create(
        createInstanceRelationships(superInstanceId, subInstanceId));
    }

    var json = instancesClient.getById(superInstanceId).getJson();
    assertThat(json.getJsonArray("parentInstances").size(), is(0));
    assertThat(json.getJsonArray("childInstances").size(), is(11));
  }

  @Test
  public void canForwardInstanceRelationshipsFetchFailure() throws Exception {
    final int expectedCount = 4;
    createSuperInstanceSubInstance(expectedCount);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Can not fetch relationships");
    instanceRelationshipClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.GET.name()));

    Response response = instancesClient
      .attemptGetMany("title=(\"super\" or \"sub\"", expectedCount);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void canFetchMultipleInstancesWithPrecedingSucceedingTitles() throws Exception {
    final int expectedCount = 200;
    final Map<String, String> precedingToSucceedingMap =
      createPrecedingSucceedingInstances(expectedCount / 2);

    final Map<String, JsonObject> precedingSucceedingInstances = instancesClient
      .getMany("title=(\"preceding\" or \"succeeding\"", expectedCount).stream()
      .collect(Collectors.toMap(json -> json.getString("id"), Function.identity()));

    assertThat(precedingSucceedingInstances.size(), is(expectedCount));

    precedingToSucceedingMap.forEach((precedingInstanceId, succeedingInstanceId) -> {
      final JsonObject precedingInstance = precedingSucceedingInstances.get(precedingInstanceId);
      final JsonObject succeedingInstance = precedingSucceedingInstances.get(succeedingInstanceId);

      verifyInstancesInPrecedingSucceedingRelationship(precedingInstance, succeedingInstance);
    });
  }

  @Test
  public void canForwardInstancePrecedingSucceedingTitlesFetchFailure() throws Exception {
    final int expectedCount = 4;
    createPrecedingSucceedingInstances(expectedCount);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    precedingSucceedingTitlesClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.GET.name()));

    Response response = instancesClient
      .attemptGetMany("title=(\"preceding\" or \"succeeding\"", expectedCount);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void canFetchMultipleInstancesWithAllRelationshipsTypes() throws Exception {
    final int expectedCount = 200;
    final Map<String, String> superInstanceToSubInstanceMap =
      createPrecedingSucceedingTitlesAndRelationshipsInstances(expectedCount / 2);

    final Map<String, JsonObject> precedingSucceedingInstances = instancesClient
      .getMany("title=(\"preceding\" or \"succeeding\"", expectedCount).stream()
      .collect(Collectors.toMap(json -> json.getString("id"), Function.identity()));

    assertThat(precedingSucceedingInstances.size(), is(expectedCount));

    superInstanceToSubInstanceMap.forEach((precedingInstanceId, succeedingInstanceId) -> {
      final JsonObject precedingInstance = precedingSucceedingInstances.get(precedingInstanceId);
      final JsonObject succeedingInstance = precedingSucceedingInstances.get(succeedingInstanceId);

      verifyInstancesInPrecedingSucceedingRelationship(precedingInstance, succeedingInstance);
      verifyInstancesInRelationship(precedingInstance, succeedingInstance);
    });
  }

  @Test
  public void cannotCreateAnInstanceWithNonExistedPrecedingTitleId()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final String precedingTitleId = UUID.randomUUID().toString();
    JsonObject precedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("precedingInstanceId", precedingTitleId);

    JsonArray precedingTitles = new JsonArray().add(precedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("precedingTitles", precedingTitles);

    Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Preceding instance does not exist",
      "precedingInstanceId", precedingTitleId));
  }

  @Test
  public void cannotCreateAnInstanceWithNonExistedSucceedingTitleId()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final String succeedingTitleId = UUID.randomUUID().toString();
    JsonObject succeedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("succeedingInstanceId", succeedingTitleId);

    JsonArray succeedingTitles = new JsonArray().add(succeedingTitle);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("succeedingTitles", succeedingTitles);

    Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Succeeding instance does not exist",
      "succeedingInstanceId", succeedingTitleId));
  }

  @Test
  public void cannotCreateAnInstanceWithNonExistedParentInstanceId()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final String superInstanceId = UUID.randomUUID().toString();
    final JsonObject parentInstance = createParentRelationship(superInstanceId,
      instanceRelationshipTypeFixture.monographicSeries().getId());

    JsonArray parentInstances = new JsonArray().add(parentInstance);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put(PARENT_INSTANCES, parentInstances);

    Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Super instance does not exist",
      "superInstanceId", superInstanceId));
  }

  @Test
  public void cannotCreateAnInstanceWithNonExistedChildInstanceId()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final String subInstanceId = UUID.randomUUID().toString();
    JsonObject childInstance = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("instanceRelationshipTypeId", instanceRelationshipTypeFixture.boundWith().getId())
      .put("subInstanceId", subInstanceId);

    JsonArray childInstances = new JsonArray().add(childInstance);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("childInstances", childInstances);

    Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Sub instance does not exist",
      "subInstanceId", subInstanceId));
  }

  @Test
  public void cannotCreateAnInstanceWithNonExistedRelationshipType()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    final String relationshipTypeId = UUID.randomUUID().toString();

    final IndividualResource subInstance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    JsonObject childInstance = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("instanceRelationshipTypeId", relationshipTypeId)
      .put("subInstanceId", subInstance.getId().toString());

    JsonArray childInstances = new JsonArray().add(childInstance);

    JsonObject smallAngryPlanetJson = smallAngryPlanet(UUID.randomUUID());
    smallAngryPlanetJson.put("childInstances", childInstances);

    Response response = instancesClient.attemptToCreate(smallAngryPlanetJson);

    assertThat(response, hasValidationError("Relationship type does not exist",
      "instanceRelationshipTypeId", relationshipTypeId));
  }

  @Test
  public void canForwardInstancePrecedingSucceedingTitlesUpdateFailure()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {
    UUID nodId = UUID.randomUUID();
    String titleId = UUID.randomUUID().toString();

    JsonObject succeedingTitle = new JsonObject()
      .put("id", titleId)
      .put("title", "A web semantic");
    JsonObject nod = nod(nodId)
      .put("hrid", "inst0006320")
      .put("succeedingTitles", new JsonArray().add(succeedingTitle));

    instancesClient.create(nod);

    final JsonObject expectedErrorResponse = new JsonObject()
      .put("message", "Server error");
    precedingSucceedingTitlesClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.PUT.name()));

    JsonObject newSucceedingTitle = new JsonObject()
      .put("id", titleId)
      .put("succeedingInstanceId", UUID.randomUUID().toString());

    JsonArray succeedingTitles = new JsonArray().add(newSucceedingTitle);

    JsonObject newNod = nod.copy();
    newNod.put("succeedingTitles", succeedingTitles);

    Response response = instancesClient.attemptToReplace(nodId, newNod);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void canForwardInstancePrecedingSucceedingTitlesDeleteFailure()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {
    UUID nodId = UUID.randomUUID();

    JsonObject succeedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("title", "A web semantic");
    JsonObject nod = nod(nodId)
      .put("hrid", "inst0006320")
      .put("succeedingTitles", new JsonArray().add(succeedingTitle));

    instancesClient.create(nod);

    final JsonObject expectedErrorResponse = new JsonObject()
      .put("message", "Server error");
    precedingSucceedingTitlesClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject newNod = nod.copy();
    newNod.put("succeedingTitles", new JsonArray());

    Response response = instancesClient.attemptToReplace(nodId, newNod);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void canForwardInstanceRelationshipUpdateFailure()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    UUID nodId = UUID.randomUUID();
    String parentInstanceId = UUID.randomUUID().toString();
    String boundWithInstanceRelationshipTypeId = instanceRelationshipTypeFixture.boundWith().getId();

    IndividualResource smallAngryPlanet = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));
    JsonObject parentInstance = new JsonObject()
      .put("id", parentInstanceId)
      .put("superInstanceId", smallAngryPlanet.getId().toString())
      .put("instanceRelationshipTypeId", boundWithInstanceRelationshipTypeId);
    JsonObject nod = nod(nodId)
      .put("hrid", "inst0006320")
      .put(PARENT_INSTANCES, new JsonArray().add(parentInstance));

    instancesClient.create(nod);

    final JsonObject expectedErrorResponse = new JsonObject()
      .put("message", "Server error");
    instanceRelationshipClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.PUT.name()));

    JsonObject newParentInstances = new JsonObject()
      .put("id", parentInstanceId)
      .put("superInstanceId", UUID.randomUUID().toString())
      .put("instanceRelationshipTypeId", boundWithInstanceRelationshipTypeId);

    JsonArray parentInstances = new JsonArray().add(newParentInstances);

    JsonObject newNod = nod.copy();
    newNod.put(PARENT_INSTANCES, parentInstances);

    Response response = instancesClient.attemptToReplace(nodId, newNod);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void canForwardInstanceRelationshipDeleteFailure()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

    UUID nodId = UUID.randomUUID();
    String boundWithInstanceRelationshipTypeId = instanceRelationshipTypeFixture.boundWith().getId();

    IndividualResource smallAngryPlanet = instancesClient.create(smallAngryPlanet(UUID.randomUUID()));

    JsonObject parentInstance = createParentRelationship(smallAngryPlanet.getId().toString(),
      boundWithInstanceRelationshipTypeId);

    JsonObject nod = nod(nodId)
      .put("hrid", "inst0006320")
      .put(PARENT_INSTANCES, new JsonArray().add(parentInstance));

    instancesClient.create(nod);

    final JsonObject expectedErrorResponse = new JsonObject()
      .put("message", "Server error");
    instanceRelationshipClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject newNod = nod.copy();
    newNod.put(PARENT_INSTANCES, new JsonArray());

    Response response = instancesClient.attemptToReplace(nodId, newNod);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson(), is(expectedErrorResponse));
  }

  @Test
  public void parentChildInstancesReturnedWhenFetchSingleInstance() throws Exception {
    final IndividualResource parentInstance = instancesClient.create(nod(UUID.randomUUID()));

    final JsonObject parentRelationship = createParentRelationship(parentInstance.getId().toString(),
      instanceRelationshipTypeFixture.boundWith().getId());

    final IndividualResource childInstance = instancesClient.create(nod(UUID.randomUUID())
      .put(PARENT_INSTANCES, new JsonArray().add(parentRelationship)));

    assertThat(childInstance.getJson().getJsonArray(PARENT_INSTANCES).getJsonObject(0),
      is(parentRelationship));

    verifyInstancesInRelationship(instancesClient.getById(parentInstance.getId()).getJson(),
      instancesClient.getById(childInstance.getId()).getJson());
  }

  @Test
  public void canUpdateInstanceWithChildInstancesWhenParentInstancesAlreadySet() throws Exception {
    final IndividualResource parentInstance = instancesClient.create(nod(UUID.randomUUID()));
    final IndividualResource childInstance = instancesClient.create(nod(UUID.randomUUID()));

    final JsonObject parentRelationship = createParentRelationship(parentInstance.getId().toString(),
      instanceRelationshipTypeFixture.boundWith().getId());

    final IndividualResource createdInstance = instancesClient.create(nod(UUID.randomUUID())
      .put(PARENT_INSTANCES, new JsonArray().add(parentRelationship)));

    assertThat(createdInstance.getJson().getJsonArray(PARENT_INSTANCES).getJsonObject(0),
      is(parentRelationship));

    final JsonObject childRelationships = createChildRelationship(childInstance.getId().toString(),
      instanceRelationshipTypeFixture.monographicSeries().getId());

    instancesClient.replace(createdInstance.getId(), createdInstance.copyJson()
      .put("childInstances", new JsonArray().add(childRelationships)));

    final JsonObject updatedInstance = instancesClient.getById(createdInstance.getId())
      .getJson();

    verifyInstancesInRelationship(instancesClient.getById(parentInstance.getId()).getJson(),
      updatedInstance);
    verifyInstancesInRelationship(updatedInstance,
      instancesClient.getById(childInstance.getId()).getJson());
  }

  @Test
  public void canLinkLocalInstanceToSharedInstance() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID parentId = UUID.randomUUID();

    initConsortiumTenant();

    final JsonObject parentRelationship = createParentRelationship(parentId.toString(),
      instanceRelationshipTypeFixture.boundWith().getId());

    final IndividualResource createdInstance = instancesClient.create(nod(UUID.randomUUID())
      .put(PARENT_INSTANCES, new JsonArray().add(parentRelationship)));

    assertThat(createdInstance.getJson().getJsonArray(PARENT_INSTANCES).getJsonObject(0),
      is(parentRelationship));

    Response getParent = instancesClient.getById(parentId);
    assertThat(getParent.getStatusCode(), is(200));

    verifyInstancesInRelationship(getParent.getJson(), createdInstance.getJson());

    List<JsonObject> allSharingInstances = consortiaClient.getAll();

    assertThat(allSharingInstances.size(), is(1));
    assertThat(allSharingInstances.get(0).getString("instanceIdentifier"), is(parentId.toString()));
  }

  @Test
  public void canUpdateLocalInstanceWithSharedInstanceRelation() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    final IndividualResource parentInstance = instancesClient.create(nod(UUID.randomUUID()));
    UUID childId = UUID.randomUUID();

    initConsortiumTenant();

    final JsonObject parentRelationship = createParentRelationship(parentInstance.getId().toString(),
      instanceRelationshipTypeFixture.boundWith().getId());

    final IndividualResource createdInstance = instancesClient.create(nod(UUID.randomUUID())
      .put(PARENT_INSTANCES, new JsonArray().add(parentRelationship)));

    assertThat(createdInstance.getJson().getJsonArray(PARENT_INSTANCES).getJsonObject(0),
      is(parentRelationship));

    final JsonObject childRelationships = createChildRelationship(childId.toString(),
      instanceRelationshipTypeFixture.monographicSeries().getId());

    instancesClient.replace(createdInstance.getId(), createdInstance.copyJson()
      .put("childInstances", new JsonArray().add(childRelationships)));

    final JsonObject updatedInstance = instancesClient.getById(createdInstance.getId())
      .getJson();

    Response getChild = instancesClient.getById(childId);
    assertThat(getChild.getStatusCode(), is(200));

    verifyInstancesInRelationship(instancesClient.getById(parentInstance.getId()).getJson(),
      updatedInstance);
    verifyInstancesInRelationship(updatedInstance, getChild.getJson());

    List<JsonObject> allSharingInstances = consortiaClient.getAll();

    assertThat(allSharingInstances.size(), is(1));
    assertThat(allSharingInstances.get(0).getString("instanceIdentifier"), is(childId.toString()));
  }

  private void initConsortiumTenant() {
    String expectedConsortiumId = UUID.randomUUID().toString();

    JsonObject userTenantsCollection = new JsonObject()
      .put(CENTRAL_TENANT_ID_FIELD, ApiTestSuite.CONSORTIA_TENANT_ID)
      .put(CONSORTIUM_ID_FIELD, expectedConsortiumId);

    userTenantsClient.create(userTenantsCollection);
  }

  private JsonObject createParentRelationship(String superInstanceId, String relationshipType) {
    return mapFrom(new InstanceRelationshipToParent(UUID.randomUUID().toString(),
      superInstanceId, relationshipType));
  }

  private JsonObject createChildRelationship(String subInstanceId, String relationshipType) {
    return mapFrom(new InstanceRelationshipToChild(UUID.randomUUID().toString(),
      subInstanceId, relationshipType));
  }

  private Map<String, String> createPrecedingSucceedingTitlesAndRelationshipsInstances(
    int count) throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final Map<String, String> precedingSucceedingInstances =
      createPrecedingSucceedingInstances(count);

    for (Map.Entry<String, String> entry : precedingSucceedingInstances.entrySet()) {
      instanceRelationshipClient.create(
        createInstanceRelationships(UUID.fromString(entry.getKey()),
          UUID.fromString(entry.getValue())));
    }

    return precedingSucceedingInstances;
  }

  private Map<String, String> createPrecedingSucceedingInstances(int count)
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final Map<String, String> map = new HashMap<>();
    for (int i = 0; i < count; i++) {
      UUID firstInstanceId = UUID.randomUUID();
      UUID secondInstanceId = UUID.randomUUID();

      instancesClient.create(nod(firstInstanceId)
        .put("title", randomString("preceding"))
        .put("identifiers", createIdentifier()));

      instancesClient.create(nod(secondInstanceId)
        .put("title", randomString("succeeding"))
        .put("identifiers", createIdentifier()));

      precedingSucceedingTitlesClient.create(
        createPrecedingSucceedingRelationship(firstInstanceId, secondInstanceId));

      map.put(firstInstanceId.toString(), secondInstanceId.toString());
    }

    return map;
  }

  private JsonArray createIdentifier() {
    return new JsonArray().add(new JsonObject()
      .put("identifierTypeId", UUID.randomUUID().toString())
      .put("value", randomString("")));
  }

  private Map<String, String> createSuperInstanceSubInstance(int count)
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final Map<String, String> map = new HashMap<>();
    for (int i = 0; i < count; i++) {
      UUID superInstanceId = UUID.randomUUID();
      UUID subInstanceId = UUID.randomUUID();

      instancesClient.create(nod(superInstanceId)
        .put("title", randomString("super")));

      instancesClient.create(nod(subInstanceId)
        .put("title", randomString("sub")));

      instanceRelationshipClient.create(
        createInstanceRelationships(superInstanceId, subInstanceId));

      map.put(superInstanceId.toString(), subInstanceId.toString());
    }

    return map;
  }

  private String randomString(String prefix) {
    return prefix + new Random().nextLong();
  }

  private JsonObject createPrecedingSucceedingRelationship(UUID precedingId, UUID succeedingId) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("precedingInstanceId", precedingId.toString())
      .put("succeedingInstanceId", succeedingId.toString());
  }

  private JsonObject createInstanceRelationships(UUID precedingId, UUID succeedingId) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("instanceRelationshipTypeId", instanceRelationshipTypeFixture.boundWith().getId())
      .put("superInstanceId", precedingId.toString())
      .put("subInstanceId", succeedingId.toString());
  }

  private void verifyInstancesInRelationship(
    JsonObject superInstance, JsonObject subInstance) {

    assertThat(superInstance, notNullValue());
    assertThat(subInstance, notNullValue());

    final JsonObject childInstances = superInstance.getJsonArray("childInstances").getJsonObject(0);
    final JsonObject parentInstances = subInstance.getJsonArray(PARENT_INSTANCES).getJsonObject(0);

    assertThat(childInstances, notNullValue());
    assertThat(parentInstances, notNullValue());

    assertThat(childInstances.getString("subInstanceId"), is(subInstance.getString("id")));
    assertThat(parentInstances.getString("superInstanceId"), is(superInstance.getString("id")));

    assertThat(parentInstances.getString("instanceRelationshipTypeId"), notNullValue());
    assertThat(parentInstances.getString("instanceRelationshipTypeId"),
      is(childInstances.getString("instanceRelationshipTypeId")));

    assertThat(parentInstances.getString("id"), notNullValue());
    assertThat(parentInstances.getString("id"), is(childInstances.getString("id")));
  }

  private void verifyInstancesInPrecedingSucceedingRelationship(
    JsonObject precedingInstance, JsonObject succeedingInstance) {

    assertThat(precedingInstance, notNullValue());
    assertThat(succeedingInstance, notNullValue());

    final JsonObject succeedingTitle = precedingInstance.getJsonArray("succeedingTitles").getJsonObject(0);
    final JsonObject precedingTitle = succeedingInstance.getJsonArray("precedingTitles").getJsonObject(0);

    assertThat(succeedingTitle, notNullValue());
    assertThat(precedingTitle, notNullValue());

    assertThat(succeedingTitle.getString("title"), is(succeedingInstance.getString("title")));
    assertThat(succeedingTitle.getString("hrid"), is(succeedingInstance.getString("hrid")));
    assertThat(succeedingTitle.getJsonArray("identifiers"),
      is(succeedingInstance.getJsonArray("identifiers")));
    assertThat(succeedingTitle.getString("succeedingInstanceId"), is(succeedingInstance.getString("id")));

    assertThat(precedingTitle.getString("title"), is(precedingInstance.getString("title")));
    assertThat(precedingTitle.getString("hrid"), is(precedingInstance.getString("hrid")));
    assertThat(precedingTitle.getJsonArray("identifiers"),
      is(precedingInstance.getJsonArray("identifiers")));
    assertThat(precedingTitle.getString("precedingInstanceId"), is(precedingInstance.getString("id")));
  }
}
