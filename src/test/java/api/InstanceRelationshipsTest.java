package api;

import static api.support.InstanceSamples.nod;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import support.fakes.EndpointFailureDescriptor;

public class InstanceRelationshipsTest extends ApiTests {
  private static final String PARENT_INSTANCES = "parentInstances";

  @After
  public void expireFailureEmulation() throws Exception {
    precedingSucceedingTitlesClient.expireFailureEmulation();
    instanceRelationshipClient.expireFailureEmulation();
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
  public void canForwardInstanceRelationshipsFetchFailure() throws Exception {
    final int expectedCount = 4;
    createSuperInstanceSubInstance(expectedCount);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Can not fetch relationships");
    instanceRelationshipClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString()));

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
        .setBody(expectedErrorResponse.toString()));

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
  public void parentChildInstancesReturnedWhenFetchSingleInstance() throws Exception {
    final IndividualResource parentInstance = instancesClient.create(nod(UUID.randomUUID()));

    final JsonObject parentRelationship = createParentRelationship(parentInstance.getId().toString(),
      UUID.randomUUID().toString());

    final IndividualResource childInstance = instancesClient.create(nod(UUID.randomUUID())
      .put(PARENT_INSTANCES, new JsonArray().add(parentRelationship)));

    assertThat(childInstance.getJson().getJsonArray(PARENT_INSTANCES).getJsonObject(0),
      is(parentRelationship));

    verifyInstancesInRelationship(instancesClient.getById(parentInstance.getId()).getJson(),
      instancesClient.getById(childInstance.getId()).getJson());
  }

  private JsonObject createParentRelationship(String superInstanceId, String relationshipType) {
    return mapFrom(new InstanceRelationshipToParent(UUID.randomUUID().toString(),
      superInstanceId, relationshipType));
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
      .put("instanceRelationshipTypeId", UUID.randomUUID().toString())
      .put("superInstanceId", precedingId.toString())
      .put("subInstanceId", succeedingId.toString());
  }

  private void verifyInstancesInRelationship(
    JsonObject superInstance, JsonObject subInstance) {

    assertThat(superInstance, notNullValue());
    assertThat(subInstance, notNullValue());

    final JsonObject childInstances = superInstance.getJsonArray("childInstances").getJsonObject(0);
    final JsonObject parentInstances = subInstance.getJsonArray("parentInstances").getJsonObject(0);

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
