package org.folio.inventory.domain.instances;

import static io.vertx.core.json.JsonObject.mapFrom;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class RelatedInstanceTest {

  @Test
  public void testRelatedInstanceFrom() {
    String id = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String relatedInstanceId = UUID.randomUUID().toString();
    String relatedInstanceTypeId = UUID.randomUUID().toString();
    RelatedInstance relatedInstance = new RelatedInstance(
        id, instanceId, relatedInstanceId, relatedInstanceTypeId);

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

}
