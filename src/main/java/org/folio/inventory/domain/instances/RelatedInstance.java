package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class RelatedInstance {
  // JSON property names
  public static final String ID_KEY = "id";
  public static final String INSTANCE_ID_KEY = "instanceId";
  public static final String RELATED_INSTANCE_ID_KEY = "relatedInstanceId";
  public static final String RELATED_INSTANCE_TYPE_ID_KEY = "relatedInstanceTypeId";

  public final String id;
  public final String instanceId;
  public final String relatedInstanceId;
  public final String relatedInstanceTypeId;

  public RelatedInstance(String id, String instanceId, String relatedInstanceId, String relatedInstanceTypeId) {
    this.id = id;
    this.instanceId = instanceId;
    this.relatedInstanceId = relatedInstanceId;
    this.relatedInstanceTypeId = relatedInstanceTypeId;
  }

  public RelatedInstance(JsonObject rel) {
    this(rel.getString(ID_KEY),
         rel.getString(INSTANCE_ID_KEY),
         rel.getString(RELATED_INSTANCE_ID_KEY),
         rel.getString(RELATED_INSTANCE_TYPE_ID_KEY));
  }

  public static RelatedInstance from(JsonObject rel, String instanceId) {
    RelatedInstance relatedInstance = new RelatedInstance(rel);

    if (!relatedInstance.instanceId.equals(instanceId)) {
      return new RelatedInstance(
        relatedInstance.id,
        instanceId,
        relatedInstance.instanceId,
        relatedInstance.relatedInstanceTypeId
      );
    }

    return relatedInstance;
  }

}
