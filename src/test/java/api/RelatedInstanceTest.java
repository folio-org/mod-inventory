package api;

import static api.support.InstanceSamples.nod;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Random;
import java.util.UUID;

import org.folio.InstanceRelationshipType;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.RelatedInstance;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RelatedInstanceTest extends ApiTests {

  private static final String SIBLING_INSTANCE_KEY = "siblingInstanceId";

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
    
    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceRelatedInstancesJson.size(), not(0));
    
    var firstInstanceFirstRelatedInstanceJson = firstInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(SIBLING_INSTANCE_KEY), is(secondInstanceId));

    var secondInstanceJson = instancesClient.getById(secondInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    
    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertThat(secondInstanceRelatedInstancesJson.size(), not(0));
    
    var secondInstanceFirstRelatedInstanceJson = secondInstanceRelatedInstancesJson.getJsonObject(0);
    
    assertThat(secondInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(SIBLING_INSTANCE_KEY), is(firstInstanceId));

  }

  private JsonObject createRelatedInstance(UUID siblingInstanceId) {
    String instanceRelationshipTypeID = UUID.randomUUID().toString();

    instanceRelationshipTypeFixture.createIfNotExist(mapFrom(
      new InstanceRelationshipType()
        .withId(instanceRelationshipTypeID)
        .withName("siblings")
      ));  

    return  mapFrom(new RelatedInstance(
        siblingInstanceId.toString(), 
        instanceRelationshipTypeID
      ));
  }

  private String randomString(String prefix) {
    return prefix + new Random().nextLong();
  }

}
