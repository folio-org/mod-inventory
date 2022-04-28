package api;

import static api.support.InstanceSamples.nod;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.RelatedInstance;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

public class RelatedInstanceTest extends ApiTests {

  @Test
  @SneakyThrows
  public void canCreateInstanceWithRelatedInstances() {
    var instanceId = UUID.randomUUID();
    var relatedInstanceId = UUID.randomUUID();

    String relatedInstanceTypeId = relatedInstanceTypeFixture.related().getId();

    var firstNodJson = nod(instanceId)
        .put(Instance.TITLE_KEY, "An instance");

    instancesClient.create(firstNodJson);

    var secondNodJson = nod(relatedInstanceId)
        .put(Instance.TITLE_KEY, "A related instance")
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), relatedInstanceId, instanceId, relatedInstanceTypeId)));

    instancesClient.create(secondNodJson);

    var firstInstanceJson = instancesClient.getById(instanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, firstInstanceRelatedInstancesJson.size());

    var firstInstanceFirstRelatedInstanceJson = firstInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(instanceId.toString()));
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    var secondInstanceJson = instancesClient.getById(relatedInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, secondInstanceRelatedInstancesJson.size());

    var secondInstanceFirstRelatedInstanceJson = secondInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(secondInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(instanceId.toString()));

    List<JsonObject> instances = instancesClient.getAll();

    Optional<JsonObject> instance = instances.stream()
        .filter(i -> i.getString("id").equals(instanceId.toString())).findAny();

    assertTrue(instance.isPresent());
    assertEquals(firstInstanceJson, instance.get());

    Optional<JsonObject> relatedInstance = instances.stream()
        .filter(i -> i.getString("id").equals(relatedInstanceId.toString())).findAny();

    assertTrue(relatedInstance.isPresent());
    assertEquals(secondInstanceJson, relatedInstance.get());
  }

  @Test
  @SneakyThrows
  public void canUpdateInstanceWithRelatedInstances() {
    var instanceId = UUID.randomUUID();
    var relatedInstanceId = UUID.randomUUID();
    var anotherRelatedInstanceId = UUID.randomUUID();

    String relatedInstanceTypeId = relatedInstanceTypeFixture.related().getId();

    var firstNodJson = nod(instanceId)
        .put(Instance.TITLE_KEY, "An instance");

    instancesClient.create(firstNodJson);

    var secondNodJson = nod(relatedInstanceId)
        .put(Instance.TITLE_KEY, "A related instance")
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), relatedInstanceId, instanceId, relatedInstanceTypeId)));

    instancesClient.create(secondNodJson);

    var thirdNodJson = nod(anotherRelatedInstanceId)
        .put(Instance.HRID_KEY, "in00000001")
        .put(Instance.TITLE_KEY, "Another related instance")
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), anotherRelatedInstanceId, relatedInstanceId,
                relatedInstanceTypeId)));

    instancesClient.create(thirdNodJson);

    var firstInstanceJson = instancesClient.getById(instanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, firstInstanceRelatedInstancesJson.size());

    var firstInstanceFirstRelatedInstanceJson = firstInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(instanceId.toString()));
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    var secondInstanceJson = instancesClient.getById(relatedInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertEquals(2, secondInstanceRelatedInstancesJson.size());

    var secondInstanceFirstRelatedInstanceJson = getRelatedInstanceById(secondInstanceRelatedInstancesJson, instanceId.toString());
    var secondInstanceSecondRelatedInstanceJson = getRelatedInstanceById(secondInstanceRelatedInstancesJson, anotherRelatedInstanceId.toString());

    assertThat(secondInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    assertThat(secondInstanceSecondRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceSecondRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(instanceId.toString()));

    assertThat(secondInstanceSecondRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(anotherRelatedInstanceId.toString()));

    var thirdInstanceJson = instancesClient.getById(anotherRelatedInstanceId).getJson();
    var thirdInstanceRelatedInstancesJson = thirdInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    var thirdInstanceFirstRelatedInstanceJson = thirdInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(thirdInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, thirdInstanceRelatedInstancesJson.size());

    assertThat(thirdInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(thirdInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(anotherRelatedInstanceId.toString()));
    assertThat(thirdInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    List<JsonObject> instances = instancesClient.getAll();

    Optional<JsonObject> instance = instances.stream()
        .filter(i -> i.getString("id").equals(instanceId.toString())).findAny();

    assertTrue(instance.isPresent());
    assertEquals(firstInstanceJson, instance.get());

    Optional<JsonObject> relatedInstance = instances.stream()
        .filter(i -> i.getString("id").equals(relatedInstanceId.toString())).findAny();

    assertTrue(relatedInstance.isPresent());
    assertEquals(secondInstanceJson, relatedInstance.get());

    Optional<JsonObject> anotherRelatedInstance = instances.stream()
        .filter(i -> i.getString("id").equals(anotherRelatedInstanceId.toString())).findAny();

    assertTrue(anotherRelatedInstance.isPresent());
    assertEquals(thirdInstanceJson, anotherRelatedInstance.get());

    thirdNodJson = thirdNodJson
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), anotherRelatedInstanceId, instanceId,
                relatedInstanceTypeId)));

    instancesClient.replace(anotherRelatedInstanceId, thirdNodJson);

    firstInstanceJson = instancesClient.getById(instanceId).getJson();
    firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    firstInstanceFirstRelatedInstanceJson = getRelatedInstanceById(firstInstanceRelatedInstancesJson, relatedInstanceId.toString());
    var firstInstanceSecondRelatedInstanceJson = getRelatedInstanceById(firstInstanceRelatedInstancesJson, anotherRelatedInstanceId.toString());

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertEquals(2, firstInstanceRelatedInstancesJson.size());

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(instanceId.toString()));

    assertThat(firstInstanceSecondRelatedInstanceJson, notNullValue());
    assertThat(firstInstanceSecondRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(instanceId.toString()));

    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));
    assertThat(firstInstanceSecondRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(anotherRelatedInstanceId.toString()));

    secondInstanceJson = instancesClient.getById(relatedInstanceId).getJson();
    secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    secondInstanceFirstRelatedInstanceJson = secondInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, secondInstanceRelatedInstancesJson.size());

    assertThat(secondInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(instanceId.toString()));

    thirdInstanceJson = instancesClient.getById(anotherRelatedInstanceId).getJson();
    thirdInstanceRelatedInstancesJson = thirdInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    thirdInstanceFirstRelatedInstanceJson = thirdInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(thirdInstanceRelatedInstancesJson, notNullValue());
    assertEquals(1, thirdInstanceRelatedInstancesJson.size());

    assertThat(thirdInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(thirdInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(anotherRelatedInstanceId.toString()));
    assertThat(thirdInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(instanceId.toString()));
  }

  @Test
  @SneakyThrows
  public void canRemoveRelatedInstancesFromInstance() {
    var instanceId = UUID.randomUUID();
    var relatedInstanceId = UUID.randomUUID();

    String relatedInstanceTypeId = relatedInstanceTypeFixture.related().getId();

    var firstNodJson = nod(instanceId)
        .put(Instance.TITLE_KEY, "An instance");

    instancesClient.create(firstNodJson);

    var secondNodJson = nod(relatedInstanceId)
        .put(Instance.TITLE_KEY, "A related instance")
        .put(Instance.HRID_KEY, "in00000001")
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), relatedInstanceId, instanceId, relatedInstanceTypeId)));

    instancesClient.create(secondNodJson);

    secondNodJson = secondNodJson
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray());

    instancesClient.replace(relatedInstanceId, secondNodJson);

    var firstInstanceJson = instancesClient.getById(instanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertEquals(0, firstInstanceRelatedInstancesJson.size());

    var secondInstanceJson = instancesClient.getById(relatedInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertEquals(0, secondInstanceRelatedInstancesJson.size());
  }

  private JsonObject createRelatedInstance(UUID id, UUID isntanceId, UUID relatedInstanceId,
      String relatedInstanceTypeId) {
    return mapFrom(new RelatedInstance(
        id.toString(), isntanceId.toString(), relatedInstanceId.toString(), relatedInstanceTypeId));
  }

  private JsonObject getRelatedInstanceById(JsonArray relatedInstances, String id) {
    for (var i = 0; i < relatedInstances.size(); i++) {
      var relatedInstance = relatedInstances.getJsonObject(i);
      if (relatedInstance.getString(RelatedInstance.INSTANCE_ID_KEY).equals(id) ||
            relatedInstance.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY).equals(id)) {
        return relatedInstance;
      }
    }
    return null;
  }

}
