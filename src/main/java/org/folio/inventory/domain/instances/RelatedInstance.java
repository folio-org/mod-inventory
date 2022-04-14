package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class RelatedInstance {
  // JSON property names
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
    this(rel.getString("id"),
         rel.getString(INSTANCE_ID_KEY),
         rel.getString(RELATED_INSTANCE_ID_KEY),
         rel.getString(RELATED_INSTANCE_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"id\": \"" + id + "\", \"instanceId\": \"" + instanceId + "\", \"relatedInstanceId\": \"" + relatedInstanceId + "\", \"relatedInstanceTypeId\": \""+ relatedInstanceTypeId + "\" }";
  }

  @Override
  public boolean equals (Object object) {
    if (object instanceof RelatedInstance) {
      return object.toString().equals(this.toString());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

}
