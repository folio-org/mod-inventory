package api;

import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static api.support.InstanceSamples.taoOfPooh;
import static api.support.InstanceSamples.uprooted;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class PrecedingSucceedingTitlesApiExamples extends ApiTests {

  @Test
  public void canCreateAnInstanceWithUnconnectedPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

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

    assertPrecedingSucceedingTitles(actualPrecedingTitle1, precedingTitles.getJsonObject(0), null,
      createdInstance.getId().toString());
    assertPrecedingSucceedingTitles(actualPrecedingTitle2, precedingTitles.getJsonObject(1), null,
      createdInstance.getId().toString());
  }

  @Test
  public void canCreateAnInstanceWithUnconnectedSucceedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

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

    assertPrecedingSucceedingTitles(actualSucceedingTitle1, succeedingTitles.getJsonObject(0),
      createdInstance.getId().toString(), null);
    assertPrecedingSucceedingTitles(actualSucceedingTitle2, succeedingTitles.getJsonObject(1),
      createdInstance.getId().toString(), null);
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
    assertPrecedingSucceedingTitles(instanceResponse.getJson().getJsonArray("precedingTitles")
        .getJsonObject(0), newPrecedingTitle, null,
      createdInstance.getId().toString());
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
    assertPrecedingSucceedingTitles(instanceResponse.getJson().getJsonArray("succeedingTitles")
        .getJsonObject(0), newSucceedingTitle, createdInstance.getId().toString(),
      null);
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
  public void canCreateAnInstanceWithConnectedPrecedingTitles()
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

    JsonArray actualPrecedingTitles = createdInstance.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, nodPrecedingTitleId);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, uprootedPrecedingTitleId);

    assertPrecedingSucceedingTitles(actualPrecedingTitle1, nod.getJson(), nod.getId().toString(),
      createdInstance.getId().toString());
    assertPrecedingSucceedingTitles(actualPrecedingTitle2, uprooted.getJson(), uprooted.getId().toString(),
      createdInstance.getId().toString());

    verifyRelatedInstanceSucceedingTitle(nod, createdInstance);
    verifyRelatedInstanceSucceedingTitle(uprooted, createdInstance);
  }

  @Test
  public void canCreateAnInstanceWithConnectedSucceedingTitles()
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

    JsonArray actualSucceedingTitles = createdInstance.getJson().getJsonArray("succeedingTitles");
    JsonObject actualSucceedingTitle1 = getRecordById(actualSucceedingTitles, nodPrecedingTitleId);
    JsonObject actualSucceedingTitle2 = getRecordById(actualSucceedingTitles, uprootedPrecedingTitleId);

    assertPrecedingSucceedingTitles(actualSucceedingTitle1, nod.getJson(),
      createdInstance.getId().toString(), nod.getId().toString());
    assertPrecedingSucceedingTitles(actualSucceedingTitle2, uprooted.getJson(),
      createdInstance.getId().toString(), uprooted.getId().toString());

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
  public void canCreateAnInstanceWithConnectedSucceedingAndPrecedingTitles()
    throws InterruptedException, MalformedURLException, TimeoutException,
    ExecutionException {

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

    JsonArray actualPrecedingTitles = createdInstance.getJson().getJsonArray("precedingTitles");
    JsonObject actualPrecedingTitle1 = getRecordById(actualPrecedingTitles, nodPrecedingTitleId);
    JsonObject actualPrecedingTitle2 = getRecordById(actualPrecedingTitles, unconnectedPrecedingTitleId);

    assertPrecedingSucceedingTitles(actualPrecedingTitle1, nod.getJson(), nod.getId().toString(),
      createdInstance.getId().toString());
    assertPrecedingSucceedingTitles(actualPrecedingTitle2, unconnectedPrecedingTitle,
      null, createdInstance.getId().toString());

    JsonArray actualSucceedingTitles = createdInstance.getJson().getJsonArray("succeedingTitles");
    JsonObject actualSucceedingTitle1 = getRecordById(actualSucceedingTitles, uprootedSucceedingTitleId);
    JsonObject actualSucceedingTitle2 = getRecordById(actualSucceedingTitles, unconnectedSucceedingTitleId);

    assertPrecedingSucceedingTitles(actualSucceedingTitle1, uprooted.getJson(),
      createdInstance.getId().toString(), uprooted.getId().toString());
    assertPrecedingSucceedingTitles(actualSucceedingTitle2, unconnectedSucceedingTitle,
      createdInstance.getId().toString(), null);

    verifyRelatedInstancePrecedingTitle(uprooted, createdInstance);
    verifyRelatedInstanceSucceedingTitle(nod, createdInstance);
  }

  @Test
  public void canCreateBatchOfInstancesWithPrecedingSucceedingTitles()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
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

    assertPrecedingSucceedingTitles(actualPrecedingTitle1, precedingTitles.getJsonObject(0), null,
      angryPlanetId.toString());
    assertPrecedingSucceedingTitles(actualPrecedingTitle2, precedingTitles.getJsonObject(1), null,
      angryPlanetId.toString());
    assertPrecedingSucceedingTitles(actualPrecedingTitle3, createdUprooted.getJson(), uprootedId.toString(),
      angryPlanetId.toString());

    JsonArray succeedingTitles = createdUprooted.getJson().getJsonArray("succeedingTitles");
    assertPrecedingSucceedingTitles(succeedingTitles.getJsonObject(0), createdAngryPlanet.getJson(),
      uprootedId.toString(), angryPlanetId.toString());
  }

  private void verifyRelatedInstancePrecedingTitle(IndividualResource relatedInstance,
    IndividualResource createdInstance) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    Response response = instancesClient.getById(relatedInstance.getId());
    JsonArray succeedingTitles = response.getJson().getJsonArray("precedingTitles");
    assertPrecedingSucceedingTitles(succeedingTitles.getJsonObject(0), createdInstance.getJson(),
      createdInstance.getId().toString(), relatedInstance.getId().toString());
  }

  private void verifyRelatedInstanceSucceedingTitle(IndividualResource relatedInstance,
    IndividualResource createdInstance) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    Response response = instancesClient.getById(relatedInstance.getId());
    JsonArray succeedingTitles = response.getJson().getJsonArray("succeedingTitles");
    assertPrecedingSucceedingTitles(succeedingTitles.getJsonObject(0), createdInstance.getJson(),
      relatedInstance.getId().toString(), createdInstance.getId().toString());
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
      .findFirst().get();
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

  private void assertPrecedingSucceedingTitles(JsonObject actualPrecedingTitle,
    JsonObject expected, String precedingInstanceId, String succeedingInstanceId) {

    assertThat(actualPrecedingTitle.getString("title"), is(expected.getString("title")));
    assertThat(actualPrecedingTitle.getString("hrid"), is(expected.getString("hrid")));
    assertThat(actualPrecedingTitle.getJsonArray("identifiers"), is(expected.getJsonArray("identifiers")));
    assertThat(actualPrecedingTitle.getString("precedingInstanceId"), is(precedingInstanceId));
    assertThat(actualPrecedingTitle.getString("succeedingInstanceId"), is(succeedingInstanceId));
  }
}
