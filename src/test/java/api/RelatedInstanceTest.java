package api;

import static api.support.InstanceSamples.nod;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Random;
import java.util.UUID;

import org.folio.inventory.domain.instances.Instance;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RelatedInstanceTest extends ApiTests {

  private static final String SIBLING_INSTANCE_KEY = "siblingInstanceId";
  private static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  @Test
  public void canFetchMultipleInstancesWithRelatedInstances() throws Exception {

  }

  @Test
  public void canFetchOneInstanceWithRelatedInstances() throws Exception {
    
    var firstInstanceId = UUID.randomUUID();
    var secondInstanceId = UUID.randomUUID();

    instancesClient.create(nod(firstInstanceId)
      .put(Instance.TITLE_KEY, randomString("first"))
      .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
        .add(createRelatedInstance(secondInstanceId)))
    );

    instancesClient.create(nod(secondInstanceId)
      .put(Instance.TITLE_KEY, randomString("second"))
      .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
        .add(createRelatedInstance(firstInstanceId)))
    );
    
    var firstInstanceJson = instancesClient.getById(firstInstanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    assertThat(firstInstanceRelatedInstancesJson.size(), is(1));
    assertThat(firstInstanceRelatedInstancesJson.getJsonObject(0).getString(SIBLING_INSTANCE_KEY), is(secondInstanceId));

    var secondInstanceJson = instancesClient.getById(secondInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    assertThat(secondInstanceRelatedInstancesJson.size(), is(1));
    assertThat(secondInstanceRelatedInstancesJson.getJsonObject(0).getString(SIBLING_INSTANCE_KEY), is(firstInstanceId));

  }

  private JsonObject createRelatedInstance(UUID siblingInstance) {
    return new JsonObject()
      .put(SIBLING_INSTANCE_KEY, siblingInstance)
      .put(INSTANCE_RELATIONSHIP_TYPE_ID_KEY, instanceRelationshipTypeFixture.createIfNotExist(new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("name", "siblings")));
  }

  private String randomString(String prefix) {
    return prefix + new Random().nextLong();
  }

}
