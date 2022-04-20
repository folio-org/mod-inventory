package api;

import static api.support.InstanceSamples.nod;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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

public class RelatedInstanceTest extends ApiTests {

  @Test
  public void testRelatedInstance() {
    String id = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String relatedInstanceId = UUID.randomUUID().toString();
    String relatedInstanceTypeId = UUID.randomUUID().toString();
    RelatedInstance relatedInstance = new RelatedInstance(
      id,
      instanceId,
      relatedInstanceId,
      relatedInstanceTypeId
    );

    JsonObject relatedInstanceJson = mapFrom(relatedInstance);

    RelatedInstance sameRelatedInstance = RelatedInstance.from(relatedInstanceJson, instanceId);

    assertEquals(id, sameRelatedInstance.id);
    assertEquals(instanceId, sameRelatedInstance.instanceId);
    assertEquals(relatedInstanceId, sameRelatedInstance.relatedInstanceId);
    assertEquals(relatedInstanceTypeId, sameRelatedInstance.relatedInstanceTypeId);

    assertEquals(relatedInstance.id, sameRelatedInstance.id);
    assertEquals(relatedInstance.instanceId, sameRelatedInstance.instanceId);
    assertEquals(relatedInstance.relatedInstanceId, sameRelatedInstance.relatedInstanceId);
    assertEquals(relatedInstance.relatedInstanceTypeId, sameRelatedInstance.relatedInstanceTypeId);

    RelatedInstance inverseRelatedInstance = RelatedInstance.from(relatedInstanceJson, relatedInstanceId);

    assertEquals(id, inverseRelatedInstance.id);
    assertEquals(relatedInstanceId, inverseRelatedInstance.instanceId);
    assertEquals(instanceId, inverseRelatedInstance.relatedInstanceId);
    assertEquals(relatedInstanceTypeId, inverseRelatedInstance.relatedInstanceTypeId);

    assertEquals(relatedInstance.id, inverseRelatedInstance.id);
    assertEquals(relatedInstance.instanceId, inverseRelatedInstance.relatedInstanceId);
    assertEquals(relatedInstance.relatedInstanceId, inverseRelatedInstance.instanceId);
    assertEquals(relatedInstance.relatedInstanceTypeId, inverseRelatedInstance.relatedInstanceTypeId);
  }

  @Test
  public void canCreateInstanceWithRelatedInstances() throws Exception {

    var instanceId = UUID.randomUUID();
    var relatedInstanceId = UUID.randomUUID();

    String relatedInstanceTypeId = relatedInstanceTypeFixture.related().getId();

    var firstNodJson = nod(instanceId)
        .put(Instance.TITLE_KEY, "An instance");

    instancesClient.create(firstNodJson);

    var thirdNodJson = nod(relatedInstanceId)
        .put(Instance.TITLE_KEY, "A related instance")
        .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
            .add(createRelatedInstance(UUID.randomUUID(), relatedInstanceId, instanceId, relatedInstanceTypeId)));

    instancesClient.create(thirdNodJson);

    var firstInstanceJson = instancesClient.getById(instanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceRelatedInstancesJson.size(), not(0));

    var firstInstanceFirstRelatedInstanceJson = firstInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.INSTANCE_ID_KEY),
        is(instanceId.toString()));
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY),
        is(relatedInstanceId.toString()));

    var secondInstanceJson = instancesClient.getById(relatedInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);

    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertThat(secondInstanceRelatedInstancesJson.size(), not(0));

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

  private JsonObject createRelatedInstance(UUID id, UUID isntanceId, UUID relatedInstanceId, String relatedInstanceTypeId) {
    return mapFrom(new RelatedInstance(
      id.toString(),
      isntanceId.toString(),
      relatedInstanceId.toString(),
      relatedInstanceTypeId
    ));
  }

}
