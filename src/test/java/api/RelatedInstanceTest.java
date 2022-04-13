package api;

import static api.support.InstanceSamples.nod;
import static io.vertx.core.json.JsonObject.mapFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Random;
import java.util.UUID;

import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.RelatedInstance;
import org.folio.inventory.support.http.client.IndividualResource;
import org.junit.Test;

import api.support.ApiTests;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RelatedInstanceTest extends ApiTests {

  @Test
  public void canFetchOneInstanceWithRelatedInstances() throws Exception {
    
    var firstInstanceId = UUID.randomUUID();
    var secondInstanceId = UUID.randomUUID();

    var firstNodJson = nod(firstInstanceId)
    .put(Instance.TITLE_KEY, randomString("first"))
    .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
      .add(createRelatedInstance(secondInstanceId)));

    instancesClient.create(firstNodJson);

    var secondNodJson = nod(secondInstanceId)
    .put(Instance.TITLE_KEY, randomString("second"))
    .put(Instance.RELATED_INSTANCES_KEY, new JsonArray()
      .add(createRelatedInstance(firstInstanceId)));

    instancesClient.create(secondNodJson);
    
    var firstInstanceJson = instancesClient.getById(firstInstanceId).getJson();
    var firstInstanceRelatedInstancesJson = firstInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    
    System.out.print("\n\n");
    System.out.print(firstInstanceJson);
    System.out.print("\n\n");
    
    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceRelatedInstancesJson.size(), not(0));
    
    var firstInstanceFirstRelatedInstanceJson = firstInstanceRelatedInstancesJson.getJsonObject(0);

    assertThat(firstInstanceRelatedInstancesJson, notNullValue());
    assertThat(firstInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY), is(secondInstanceId.toString()));

    var secondInstanceJson = instancesClient.getById(secondInstanceId).getJson();
    var secondInstanceRelatedInstancesJson = secondInstanceJson.getJsonArray(Instance.RELATED_INSTANCES_KEY);
    
    assertThat(secondInstanceRelatedInstancesJson, notNullValue());
    assertThat(secondInstanceRelatedInstancesJson.size(), not(0));
    
    var secondInstanceFirstRelatedInstanceJson = secondInstanceRelatedInstancesJson.getJsonObject(0);
    
    assertThat(secondInstanceFirstRelatedInstanceJson, notNullValue());
    assertThat(secondInstanceFirstRelatedInstanceJson.getString(RelatedInstance.RELATED_INSTANCE_ID_KEY), is(firstInstanceId.toString()));

  }

  private JsonObject createRelatedInstance(UUID relatedInstanceId) {
    return  mapFrom(new RelatedInstance(
        relatedInstanceId.toString()
      ));
  }

  private String randomString(String prefix) {
    return prefix + new Random().nextLong();
  }

}
